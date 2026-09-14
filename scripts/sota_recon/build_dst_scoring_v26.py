"""
sota_recon/build_dst_scoring_v26.py  --  compute the DST scoring layer on v26 (was uncomputed).

DEF rows had empty pts_def_* (so rank_def + DEF LAMAR were null). Compute from the reconciled atoms:
  modular components (raw counts, x1, for per-league import):
    pts_def_sack=def_sacks, pts_def_int=def_interceptions, pts_def_ff=def_fumbles_forced,
    pts_def_fr=fum_rec, pts_def_td=def_tds+fum_ret_td, pts_def_safety=def_safeties,
    pts_def_block=fg_blocked, pts_def_tfl=def_tackles_for_loss, pts_def_3out=three_out,
    pts_def_4stop=fourth_down_stop
  DST fumble recoveries:
    DEF-row fum_rec/def_fumbles = opponent team Fumbles-Lost, not player_defense fumbles_rec
    (the latter includes own-team offensive/ST recoveries and belongs on individual rows)
  scored (standard, matches fantasy_points_calculator):
    def_base = sack*1 + int*2 + fr*2 + td*6 + safety*2 + block*2
    pa_tiers = pa0*10 + pa1_6*7 + pa7_13*4 + pa14_20*1 + pa28_34*-1 + pa35+*-4
    ya_tiers = neg*5 + 0_99*4 + 100_199*3 + 200_299*2 + 400_449*-2 + 450_499*-2 + 500_549*-4 + 550+*-4
    pts_def_std = def_base + pa_tiers ; pts_def_ya = def_base + pa_tiers + ya_tiers
DEF rows only (leak guard: NULL/0 elsewhere). Gated: golden_samples 24/24, rows unchanged, pts_def_std populated on DEF.

    python -m scripts.sota_recon.build_dst_scoring_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp
TEAM_GAMES = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
TEAM_STATS = "D:/league-history-data/nfl/raw/pfr/boxscores/tables/team_stats/_combined.parquet"
PROV="wave41.dst_scoring"; FUM_REC_PROV="wave50.dst_fum_rec_from_team_stats"; PROV_COL="recon_correction_log"

FPTS_COLS = [
    "fpts_4pt_0ppr", "fpts_4pt_half", "fpts_4pt_ppr",
    "fpts_5pt_0ppr", "fpts_5pt_half", "fpts_5pt_ppr",
    "fpts_6pt_0ppr", "fpts_6pt_half", "fpts_6pt_ppr",
    "fpts_4pt_tep", "fpts_5pt_tep", "fpts_6pt_tep",
    "fpts_4pt_ppfd", "fpts_5pt_ppfd", "fpts_6pt_ppfd",
    "fpts_4pt_0ppr_ret", "fpts_4pt_half_ret", "fpts_4pt_ppr_ret",
    "fpts_5pt_0ppr_ret", "fpts_5pt_half_ret", "fpts_5pt_ppr_ret",
    "fpts_6pt_0ppr_ret", "fpts_6pt_half_ret", "fpts_6pt_ppr_ret",
    "fpts_4pt_tep_ret", "fpts_5pt_tep_ret", "fpts_6pt_tep_ret",
]
D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"
# De-duped defensive TDs (mirrors fantasy_points_calculator.pts_def_td): NFLverse `def_tds` ALREADY
# includes fumble-return TDs on most post-1999 rows, so `def_tds + fum_ret_td` DOUBLE-COUNTS them (1,343
# DEF rows; e.g. Raiders 2023 wk15 read 3 TDs instead of 2). Use GREATEST of the disjoint component sum
# (int_ret_td + fum_ret_td) and the box-score aggregate def_tds. Taking the MAX (never adding) avoids the
# double-count in modern data (where def_tds <= component sum, so the sum wins) AND stops silently
# dropping TDs in pre-2000 PFR box scores where def_tds is authoritative but the play-by-play split is
# incomplete (e.g. 1950 Lions wk1: box def_tds=3, only 1 int_ret_td classified -> was crediting 1, -12 pts).
TD_DEDUP = f"GREATEST({D('def_int_ret_td')}+{D('fum_ret_td')}, {D('def_tds')})"
COMPONENTS = {
    "pts_def_sack": D("def_sacks"), "pts_def_int": D("def_interceptions"), "pts_def_ff": D("def_fumbles_forced"),
    "pts_def_fr": D("fum_rec"), "pts_def_td": TD_DEDUP, "pts_def_safety": D("def_safeties"),
    "pts_def_tfl": D("def_tackles_for_loss"), "pts_def_3out": D("three_out"), "pts_def_4stop": D("fourth_down_stop"),
}
# pts_def_block (team blocked kicks) is NOT a simple atom-on-the-DEF-row: fg_blocked lives on the
# opponent's kicker. It is pre-populated below by aggregating individual defenders' def_blk_kick to the
# team. pts_def_std must equal scoring_config._defense_correction_term's baked baseline:
# sack1/int2/fr2/td6/safe2/blk2 + ret_td6 + PA. Forced fumbles stay in pts_def_ff for custom leagues,
# but are not baked into the standard DST baseline.
# ST return TD (pts_def_ret_td) IS baked at +6: 89% of league-years award a DST return TD (median 6),
# so the modal baseline must include it. _defense_correction_term shifts its return-TD baseline 0->6 in
# lockstep, so per-league totals stay invariant (pts_def_std cancels in the correction). pts_def_ret_td
# is a special-teams (KR/PR) count, DISJOINT from TD_DEDUP (defensive INT/fum/blk returns) -> no double-count.
DEF_BASE = f"({D('def_sacks')}*1 + {D('def_interceptions')}*2 + {D('fum_rec')}*2 + {TD_DEDUP}*6 + {D('def_safeties')}*2 + {D('pts_def_block')}*2 + {D('pts_def_ret_td')}*6)"
PA = f"({D('pts_allow_0')}*10 + {D('pts_allow_1_6')}*7 + {D('pts_allow_7_13')}*4 + {D('pts_allow_14_20')}*1 + {D('pts_allow_28_34')}*-1 + {D('pts_allow_35_plus')}*-4)"
_ya = D("total_yds_allowed")
YA = (f"(CASE WHEN {_ya}<0 THEN 5 WHEN {_ya}<=99 THEN 4 WHEN {_ya}<=199 THEN 3 WHEN {_ya}<=299 THEN 2 "
      f"WHEN {_ya}<=399 THEN 0 WHEN {_ya}<=449 THEN -2 WHEN {_ya}<=499 THEN -2 WHEN {_ya}<=549 THEN -4 ELSE -4 END)")


def _apply_dst_fumble_recoveries(con) -> int:
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dst_fum_rec AS
        SELECT
          CAST(g.year AS INT) AS yr,
          CAST(g.week AS INT) AS wk,
          CAST(g.team_fid AS INT) AS fr,
          CAST(g.opponent_fid AS INT) AS opp_fr,
          CAST(g.game_date AS DATE) AS game_dt,
          CAST(g.boxscore_id AS VARCHAR) AS boxscore_id,
          COALESCE(
            TRY_CAST(
              regexp_extract(
                CAST(CASE WHEN g.is_home THEN ts.vis_stat ELSE ts.home_stat END AS VARCHAR),
                '([0-9]+)\\s*-\\s*([0-9]+)',
                2
              ) AS DOUBLE
            ),
            0
          ) AS opponent_fumbles_lost
        FROM read_parquet('{TEAM_GAMES}') g
        JOIN read_parquet('{TEAM_STATS}') ts
          ON ts.boxscore_id = g.boxscore_id
        WHERE g.team_fid IS NOT NULL
          AND g.year IS NOT NULL
          AND g.week IS NOT NULL
          AND ts.stat IN ('Fumbles-Lost', 'Fumbles Lost')
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE dst_fum_match AS
        WITH dst AS (
          SELECT
            rowid AS rid,
            CAST(year AS INT) AS yr,
            CAST(week AS INT) AS wk,
            CAST(nfl_franchise_number AS INT) AS fr,
            CAST(opponent_nfl_franchise_number AS INT) AS opp_fr,
            TRY_CAST(game_date AS DATE) AS game_dt,
            COALESCE(CAST(player_week AS VARCHAR), '') AS player_week
          FROM st
          WHERE position = 'DEF'
        ),
        unique_week_opp AS (
          SELECT
            yr,
            wk,
            fr,
            opp_fr,
            ANY_VALUE(opponent_fumbles_lost) AS opponent_fumbles_lost
          FROM dst_fum_rec
          GROUP BY yr, wk, fr, opp_fr
          HAVING COUNT(*) = 1
        ),
        candidates AS (
          SELECT d.rid, f.opponent_fumbles_lost, 1 AS priority
          FROM dst d
          JOIN dst_fum_rec f
            ON f.boxscore_id IS NOT NULL
           AND f.boxscore_id <> ''
           AND d.yr = f.yr
           AND d.fr = f.fr
           AND strpos(d.player_week, f.boxscore_id) > 0

          UNION ALL

          SELECT d.rid, f.opponent_fumbles_lost, 2 AS priority
          FROM dst d
          JOIN dst_fum_rec f
            ON d.yr = f.yr
           AND d.fr = f.fr
           AND d.game_dt IS NOT NULL
           AND d.game_dt = f.game_dt
           AND d.opp_fr IS NOT DISTINCT FROM f.opp_fr

          UNION ALL

          SELECT d.rid, f.opponent_fumbles_lost, 3 AS priority
          FROM dst d
          JOIN unique_week_opp f
            ON d.yr = f.yr
           AND d.wk = f.wk
           AND d.fr = f.fr
           AND d.opp_fr IS NOT DISTINCT FROM f.opp_fr
        ),
        ranked AS (
          SELECT
            rid,
            opponent_fumbles_lost,
            ROW_NUMBER() OVER (
              PARTITION BY rid
              ORDER BY priority, opponent_fumbles_lost DESC
            ) AS rn
          FROM candidates
        )
        SELECT rid, opponent_fumbles_lost
        FROM ranked
        WHERE rn = 1
    """)
    updated = con.execute("""
        SELECT COUNT(*)
        FROM st
        JOIN dst_fum_match f
          ON st.rowid = f.rid
        WHERE st.position = 'DEF'
          AND (
            COALESCE(TRY_CAST(st.fum_rec AS DOUBLE), -1) <> f.opponent_fumbles_lost
            OR COALESCE(TRY_CAST(st.def_fumbles AS DOUBLE), -1) <> f.opponent_fumbles_lost
          )
    """).fetchone()[0]
    con.execute(f"""
        UPDATE st
        SET fum_rec = f.opponent_fumbles_lost,
            def_fumbles = f.opponent_fumbles_lost,
            {PROV_COL} = CASE
              WHEN {PROV_COL} IS NULL OR {PROV_COL} = '' THEN '{FUM_REC_PROV}'
              WHEN NOT list_contains(string_split({PROV_COL}, ','), '{FUM_REC_PROV}')
                THEN {PROV_COL} || ',{FUM_REC_PROV}'
              ELSE {PROV_COL}
            END
        FROM dst_fum_match f
        WHERE st.position = 'DEF'
          AND st.rowid = f.rid
    """)
    return int(updated or 0)


def run(apply=False):
    v26=latest_v26()
    if not apply:
        con=duckdb.connect(); con.execute("SET memory_limit='5GB'")
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{v26}') WHERE position='DEF'").fetchone()[0]
    stamp=utc_stamp(); sp=os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp,exist_ok=True)
    con=duckdb.connect(os.path.join(sp,"work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    # The wide 1089-col super table materialization OOM'd at 6GB when other jobs competed on the
    # local box; RECON_MEMORY_LIMIT=3GB (with temp_directory spill) bounds peak memory and spills
    # earlier. Kept as an override rather than the default so unconstrained runs stay fast.
    con.execute(f"SET memory_limit='{os.environ.get('RECON_MEMORY_LIMIT', '6GB')}'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute("SET max_temp_directory_size='40GB'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    existing=set(c[0] for c in con.execute("DESCRIBE st").fetchall())
    for c in list(COMPONENTS)+["pts_def_std","pts_def_ya","fum_rec","def_fumbles"]+FPTS_COLS:
        if c not in existing: con.execute(f"ALTER TABLE st ADD COLUMN {c} DOUBLE")
    if PROV_COL not in existing: con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    dst_fum_updated = _apply_dst_fumble_recoveries(con)
    # ST-return components (consumed by scoring_config: def_st_td/kr_td/pr_td -> pts_def_ret_td,
    # def_st_yd/kr_yd/pr_yd -> pts_def_ret_yd). pts_def_ret_td (return TDs) IS baked into pts_def_std
    # below at +6 (modal); the correction shifts its baseline 0->6 so per-league totals are invariant.
    # pts_def_ret_yd (return yards) stays purely per-league additive. special_teams_tds + return yards
    # live on individual returners; aggregate to the team and stamp onto the DEF row.
    # pts_def_block (team blocked kicks) = SUM of individual defenders' def_blk_kick (FG/punt/XP blocks).
    # fg_blocked lives on the opponent's kicker, not the DEF row -> defender-side def_blk_kick is the clean
    # credit (one per block; no double-count). 1978+.
    for col in ("pts_def_ret_td", "pts_def_ret_yd", "pts_def_block"):
        if col not in existing: con.execute(f"ALTER TABLE st ADD COLUMN {col} DOUBLE")
    con.execute("UPDATE st SET pts_def_ret_td=0, pts_def_ret_yd=0, pts_def_block=0 WHERE position='DEF'")
    con.execute("""UPDATE st SET pts_def_ret_td=agg.sttd, pts_def_ret_yd=agg.styd, pts_def_block=agg.blk FROM (
        SELECT nfl_team, year, week,
          SUM(COALESCE(TRY_CAST(special_teams_tds AS DOUBLE),0)) sttd,
          SUM(COALESCE(TRY_CAST(kickoff_return_yards AS DOUBLE),0)+COALESCE(TRY_CAST(punt_return_yards AS DOUBLE),0)) styd,
          SUM(COALESCE(TRY_CAST(def_blk_kick AS DOUBLE),0)) blk
        FROM st WHERE position<>'DEF' AND nfl_team IS NOT NULL GROUP BY nfl_team, year, week) agg
        WHERE st.position='DEF' AND st.nfl_team=agg.nfl_team AND st.year=agg.year AND st.week=agg.week""")
    # PRE-1978: def_blk_kick doesn't exist; the only recoverable block is a blocked PUNT (punts_blocked,
    # 1934+, on the opponent's punter). Credit it to the BLOCKING defense via opponent mapping. Gated
    # year<1978 so it never overlaps def_blk_kick (1978+) -> no double-count. (FG/XP blocks pre-1999 have
    # no source.) 94 blocked punts 1934-77.
    con.execute("""UPDATE st SET pts_def_block = COALESCE(pts_def_block,0) + opp.pb FROM (
        SELECT year, week, nfl_team AS punting_team, SUM(COALESCE(TRY_CAST(punts_blocked AS DOUBLE),0)) pb
        FROM st WHERE punts_blocked IS NOT NULL AND nfl_team IS NOT NULL GROUP BY year, week, nfl_team) opp
        WHERE st.position='DEF' AND st.year<1978 AND st.year=opp.year AND st.week=opp.week
          AND st.opponent_nfl_team=opp.punting_team""")
    # Ancient PA-tier gap (Joe 2026-07-15): a handful of DEF rows (1944-52) have dst_points_allowed NULL
    # while points_allowed>0 -> they were never PA-classified, so NO pts_allow_* flag is set -> the PA term
    # is silently 0 (missing tier scoring). Populate from RAW points_allowed (pre-modern: opponent
    # return-TD/safety contraction is negligible/unwitnessed) and set the matching tier flag. Filtered on
    # dst_points_allowed IS NULL so the CORRECT safety/pick-six shutouts (dst_pa=0 with pts_allow_0 already
    # set) and the legit 21-27 zero-point tier (dst_pa>0, no flag) are left untouched.
    _pa_flag = {
        "pts_allow_0":       f"CASE WHEN {D('points_allowed')}=0 THEN 1 ELSE 0 END",
        "pts_allow_1_6":     f"CASE WHEN {D('points_allowed')} BETWEEN 1 AND 6 THEN 1 ELSE 0 END",
        "pts_allow_7_13":    f"CASE WHEN {D('points_allowed')} BETWEEN 7 AND 13 THEN 1 ELSE 0 END",
        "pts_allow_14_20":   f"CASE WHEN {D('points_allowed')} BETWEEN 14 AND 20 THEN 1 ELSE 0 END",
        "pts_allow_21_27":   f"CASE WHEN {D('points_allowed')} BETWEEN 21 AND 27 THEN 1 ELSE 0 END",
        "pts_allow_28_34":   f"CASE WHEN {D('points_allowed')} BETWEEN 28 AND 34 THEN 1 ELSE 0 END",
        "pts_allow_35_plus": f"CASE WHEN {D('points_allowed')} >= 35 THEN 1 ELSE 0 END",
    }
    _pa_set = ", ".join(f"{c}={e}" for c, e in _pa_flag.items() if c in existing)
    con.execute(f"""UPDATE st SET dst_points_allowed={D('points_allowed')}, {_pa_set}
        WHERE position='DEF' AND dst_points_allowed IS NULL AND {D('points_allowed')}>0""")
    setc=", ".join(f"{c}={expr}" for c,expr in COMPONENTS.items())
    con.execute(f"""UPDATE st SET {setc}, pts_def_std={DEF_BASE}+{PA}, pts_def_ya={DEF_BASE}+{PA}+{YA},
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        WHERE position='DEF'""")
    fpts_set = ", ".join(f"{c}=pts_def_std" for c in FPTS_COLS)
    con.execute(f"UPDATE st SET {fpts_set} WHERE position='DEF'")
    after=con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    nstd=con.execute("SELECT COUNT(*) FROM st WHERE position='DEF' AND pts_def_std IS NOT NULL AND pts_def_std<>0").fetchone()[0]
    rng=con.execute("SELECT ROUND(MIN(pts_def_std),1),ROUND(MAX(pts_def_std),1),ROUND(AVG(pts_def_std),2) FROM st WHERE position='DEF'").fetchone()
    fpts_bad=con.execute("SELECT COUNT(*) FROM st WHERE position='DEF' AND ABS(COALESCE(fpts_4pt_half,0)-COALESCE(pts_def_std,0))>0.01").fetchone()[0]
    vp=Path(v26); tmp=vp.with_name(vp.stem+"_dst.parquet")
    r=con.execute("SELECT * FROM st").fetch_record_batch(50000); w=pq.ParquetWriter(str(tmp),r.schema)
    for b in r: w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp,ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o=S.latest_v26; S.latest_v26=lambda:str(tmp)
    try: g=golden_samples.run()
    finally: S.latest_v26=o
    gate=(g["failed"]==0) and (after==before) and nstd>0 and fpts_bad==0
    res={"before":before,"after":after,"dst_fum_updated":dst_fum_updated,"def_std_nonzero":nstd,"std_range":rng,"def_fpts_mismatch":fpts_bad,"golden":f"{g['passed']}/{g['total']}","gate_pass":bool(gate),"temp":str(tmp)}
    if gate:
        bk=vp.with_name(vp.stem+f"_predst_{stamp}.parquet"); shutil.copy2(vp,bk); __import__("scripts.sota_recon.recon_common",fromlist=["safe_replace"]).safe_replace(tmp,vp); res["backup"]=str(bk); res["swapped"]=True
    else: res["swapped"]=False
    return res


if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--apply",action="store_true"); a=ap.parse_args()
    if not a.apply:
        print(f"would score {run():,} DEF rows")
    else:
        r=run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | dst_fum_rec_updates={r['dst_fum_updated']:,} | pts_def_std nonzero={r['def_std_nonzero']:,} range(min/max/avg)={r['std_range']} | def_fpts_mismatch={r['def_fpts_mismatch']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "+("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
