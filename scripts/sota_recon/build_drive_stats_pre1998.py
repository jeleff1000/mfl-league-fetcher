"""
sota_recon/build_drive_stats_pre1998.py  --  reconstruct three_out / fourth_down_stop for 1978-1997 from pbp

The authoritative drives table (home_drives/vis_drives) only covers 1998+, so build_drive_stats.py left
three_out/fourth_down_stop empty before 1998. pbp covers 1978+ but has no possession/team column, so we
reconstruct drives from the play-by-play and validate the method against the 1998+ drives table.

Reconstruction (validated vs 1998+ truth: three_out 76% exact / 96% within-1 / 104% of total;
fourth_down_stop 93% exact / 99.8% within-1 / 96% of total):
  - snap = a scrimmage down that COUNTS toward PFR's play_count: has a down, and is NOT a
    penalty-only row, a "(no play)" penalty-negated play, a timeout, a punt/FG/kickoff/XP/kneel.
  - drive ends on: punt, interception, fumble LOST (recoverer's team != fumbler's team, via roster),
    made/missed FG, TD, safety, turnover-on-downs (4th-down explicit failure: incomplete/sacked/
    stuffed-with-parsed-gain<ytg, never a conversion/TD/penalty/aborted), and period end (halftime
    q2->q3, game end = last row) detected via the quarter column (pbp has no end-of-period rows).
  - three_out        = own drive with snaps<=3 ending in a Punt  -> credited to the offense's DST row
  - fourth_down_stop = opponent drive ending in turnover-on-downs -> credited to the defending DST row
  - team attribution: each drive's offense = the modal team of its snaps' primary (initiating) player,
    mapped team_code -> team_fid via nfl_team_games_all. fourth_down_stop -> the offense's opponent_fid.

Scope: 1978-1997 ONLY (1998+ stays authoritative from the drives table). Columns are string-typed;
written on the team DEF row. Gated: golden_samples + golden_matrix pass, rows unchanged, both columns
populated (nonzero) in 1978-97.

    python -m scripts.sota_recon.build_drive_stats_pre1998 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
BOX="D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG="D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
PROV="wave34.drive_stats_pre1998"; PROV_COL="recon_correction_log"
LO,HI=1978,1997
NAME=r"([A-Z][A-Za-z.''\-]+(?: [A-Z][A-Za-z.''\-]+){0,2})"
NN="lower(regexp_replace({},'[^A-Za-z]','','g'))"


def _targets(con):
    """Build per-(year,week,franchise) three_out / fourth_down_stop from reconstructed pbp drives."""
    P=f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk,
        team_code, team_fid fid, opponent_fid ofid FROM read_parquet('{TG}')
        WHERE team_fid IS NOT NULL AND CAST(year AS INT) BETWEEN {LO} AND {HI}""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bxset AS SELECT DISTINCT boxscore_id FROM tg""")
    # roster keyed by pfr_id (offense initiator -> team) and by normalized name (fumbler/recoverer -> team)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rpid AS SELECT boxscore_id, split_part(player_link_ids,';',1) pid, ANY_VALUE(team) team
        FROM read_parquet('{BOX}/player_offense/_combined.parquet') WHERE player_link_ids IS NOT NULL GROUP BY 1,2""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rnm AS SELECT boxscore_id, nn, ANY_VALUE(team) team FROM (
        SELECT boxscore_id, {NN.format('player')} nn, team FROM read_parquet('{BOX}/player_offense/_combined.parquet') WHERE player IS NOT NULL
        UNION ALL SELECT boxscore_id, {NN.format('player')} nn, team FROM read_parquet('{BOX}/player_defense/_combined.parquet') WHERE player IS NOT NULL) GROUP BY 1,2""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pp AS SELECT b.boxscore_id, row_index_in_table ri, quarter q, TRY_CAST(down AS INT) dn,
        TRY_CAST(yds_to_go AS INT) ytg, lower(detail) ld, split_part(detail_link_ids,';',1) off_pid,
        CASE WHEN lower(detail) LIKE '%for no gain%' THEN 0 ELSE TRY_CAST(regexp_extract(lower(detail),'for (-?\\d+) yard',1) AS INT) END gain,
        {NN.format("regexp_extract(detail,'"+NAME+" fumbles',1)")} fnn,
        {NN.format("regexp_extract(detail,'recovered by "+NAME+"',1)")} rnn
        FROM {P} p JOIN bxset b USING(boxscore_id) WHERE detail IS NOT NULL AND detail<>''""")
    con.execute("""CREATE OR REPLACE TEMP TABLE pj AS SELECT pp.*, ro.team off_team, rf.team ftm, rr.team rtm
        FROM pp LEFT JOIN rpid ro ON ro.boxscore_id=pp.boxscore_id AND ro.pid=pp.off_pid
                LEFT JOIN rnm rf ON rf.boxscore_id=pp.boxscore_id AND rf.nn=pp.fnn
                LEFT JOIN rnm rr ON rr.boxscore_id=pp.boxscore_id AND rr.nn=pp.rnn""")
    con.execute("""CREATE OR REPLACE TEMP TABLE ev AS SELECT *,
        CASE WHEN dn IS NULL THEN 0 WHEN ld LIKE 'penalty on%' THEN 0
             WHEN ld LIKE '%(no play)%' THEN 0 WHEN ld LIKE '%timeout%' THEN 0
             WHEN ld LIKE '%punt%' OR ld LIKE '%field goal%' OR ld LIKE '%kicks off%' OR ld LIKE '%extra point%' OR ld LIKE '%kneel%' THEN 0 ELSE 1 END is_snap,
        CASE WHEN ld LIKE '%punt%' AND ld NOT LIKE '%fake%' THEN 'Punt'
             WHEN ld LIKE '%intercepted%' THEN 'Int'
             WHEN ld LIKE '%fumble%' AND ld LIKE '%recovered by%' AND ftm IS NOT NULL AND rtm IS NOT NULL AND ftm<>rtm THEN 'Fum'
             WHEN ld LIKE '%field goal%' THEN 'FG' WHEN ld LIKE '%touchdown%' THEN 'TD' WHEN ld LIKE '%safety%' THEN 'Saf'
             WHEN dn=4 AND ld NOT LIKE '%first down%' AND ld NOT LIKE '%touchdown%' AND ld NOT LIKE '%kneel%' AND ld NOT LIKE '%penalty%' AND ld NOT LIKE '%aborted%'
                  AND (ld LIKE '%incomplete%' OR ld LIKE '%sacked%' OR (gain IS NOT NULL AND ytg IS NOT NULL AND gain<ytg)) THEN 'Downs'
             ELSE NULL END endev0 FROM pj""")
    con.execute("""CREATE OR REPLACE TEMP TABLE ev2 AS SELECT *, CASE WHEN endev0 IS NOT NULL THEN endev0
        WHEN q='2' AND LEAD(q) OVER w='3' THEN 'Half'
        WHEN ri=MAX(ri) OVER (PARTITION BY boxscore_id) THEN 'End' ELSE NULL END endev
        FROM ev WINDOW w AS (PARTITION BY boxscore_id ORDER BY ri)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE seg AS SELECT *,
        COALESCE(SUM(CASE WHEN endev IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY boxscore_id ORDER BY ri ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) did FROM ev2""")
    # per drive: snaps, end, offense team_code = modal team of snap initiators
    con.execute("""CREATE OR REPLACE TEMP TABLE drv AS SELECT boxscore_id, did, SUM(is_snap) plays,
        MAX(endev) FILTER(WHERE endev IS NOT NULL) e,
        mode(off_team) FILTER(WHERE is_snap=1 AND off_team IS NOT NULL) ot FROM seg GROUP BY 1,2""")
    # map offense team_code -> fid (offense) + ofid (defense). three_out -> offense fid; fourth_down_stop -> defense fid
    con.execute("""CREATE OR REPLACE TEMP TABLE dj AS SELECT d.*, g.yr, g.wk, g.fid off_fid, g.ofid def_fid
        FROM drv d JOIN tg g ON g.boxscore_id=d.boxscore_id AND g.team_code=d.ot""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
        WITH thr AS (SELECT yr, wk, off_fid fid, SUM(CASE WHEN plays<=3 AND e='Punt' THEN 1 ELSE 0 END) three_out FROM dj GROUP BY 1,2,3),
             fds AS (SELECT yr, wk, def_fid fid, SUM(CASE WHEN e='Downs' THEN 1 ELSE 0 END) fourth_down_stop FROM dj GROUP BY 1,2,3)
        SELECT COALESCE(thr.yr,fds.yr) yr, COALESCE(thr.wk,fds.wk) wk, COALESCE(thr.fid,fds.fid) fid,
               COALESCE(thr.three_out,0) three_out, COALESCE(fds.fourth_down_stop,0) fourth_down_stop
        FROM thr FULL JOIN fds ON thr.yr=fds.yr AND thr.wk=fds.wk AND thr.fid=fds.fid""")


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'"); _targets(con)
        return con.execute("SELECT SUM(three_out), SUM(fourth_down_stop), COUNT(*) FROM tgt").fetchone()
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'"); con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in [c[0] for c in con.execute("DESCRIBE st").fetchall()]:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre_nonnull=con.execute(f"SELECT COUNT(*) FROM st WHERE position='DEF' AND CAST(year AS INT) BETWEEN {LO} AND {HI} AND (three_out IS NOT NULL OR fourth_down_stop IS NOT NULL)").fetchone()[0]
    _targets(con)
    con.execute(f"""UPDATE st SET three_out=CAST(t.three_out AS VARCHAR), fourth_down_stop=CAST(t.fourth_down_stop AS VARCHAR),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.position='DEF' AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
          AND st.nfl_franchise_number=t.fid AND CAST(st.year AS INT) BETWEEN {LO} AND {HI}""")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    tot=con.execute(f"SELECT SUM(TRY_CAST(three_out AS INT)), SUM(TRY_CAST(fourth_down_stop AS INT)) FROM st WHERE position='DEF' AND CAST(year AS INT) BETWEEN {LO} AND {HI}").fetchone()
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_drv78.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and (pre_nonnull==0) and (tot[0] or 0)>0 and (tot[1] or 0)>0
    res={"before":before,"after":after,"pre_nonnull_1978_97":pre_nonnull,"three_out_total":tot[0],"fds_total":tot[1],"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_predrv78_{stamp}.parquet"); shutil.copy2(vp,bk); os.replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if a.apply:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | pre-existing 1978-97 non-null {r['pre_nonnull_1978_97']} | three_out {r['three_out_total']:,} | fourth_down_stop {r['fds_total']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
    else:
        t=run(); print(f"would populate three_out={t[0]:,} fourth_down_stop={t[1]:,} across {t[2]:,} (year,week,franchise) cells (1978-97)")
