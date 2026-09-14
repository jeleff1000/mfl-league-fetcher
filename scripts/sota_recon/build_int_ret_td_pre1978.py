"""
sota_recon/build_int_ret_td_pre1978.py  --  close the pre-1978 def_int_ret_td gap from scoring

The scoring witness exposed that v26 def_int_ret_td (interception returned for TD) is ~9% low
pre-1978 vs the play-description record: player_defense.def_int_td (what the reconcile used) is
incomplete in that era, but the scoring table names every INT-return TD AND links the scorer.

This attributes each pre-1978 "interception return" scoring play to its scorer (the FIRST
description link), maps that pfr_id -> NFL_player_id via player_bio, derives the scoring team via
the running-score delta, and sets def_int_ret_td on the matching player-week (overwrite to the
scoring count where scoring has it -- scoring is the authority for "did this TD happen").

Gated: golden 24/24, def_int_ret_td pre-1978 team-game agreement vs scoring rises toward ~99%,
row count unchanged (scorers already exist as IDP rows from their interception) -> backup + swap.

    python -m scripts.sota_recon.build_int_ret_td_pre1978            # dry-run
    python -m scripts.sota_recon.build_int_ret_td_pre1978 --apply
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave22.int_ret_td_pre1978_from_scoring"
PROV_COL = "recon_correction_log"
YEAR_HI = 1977


def _build_target(con):
    """scoring INT-return-TD events pre-1978 -> (NFL_player_id, yr, wk) count, team via score-delta."""
    SC = f"read_parquet('{BOX}/scoring/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr, is_home
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS
        SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}')
        WHERE pfr_id IS NOT NULL GROUP BY 1""")
    # INT-return TD scoring events pre-1978, scorer = first description link, team via score delta
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ev AS
        SELECT boxscore_id, split_part(description_link_ids,';',1) scorer,
          TRY_CAST(vis_team_score AS INT) - LAG(TRY_CAST(vis_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dvis,
          TRY_CAST(home_team_score AS INT) - LAG(TRY_CAST(home_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dhome
        FROM {SC}
        WHERE lower(description) LIKE '%interception return%' AND description_link_ids IS NOT NULL
          AND CAST(season AS INT) <= {YEAR_HI}""")
    con.execute("""CREATE OR REPLACE TEMP TABLE evt AS
        SELECT e.boxscore_id, e.scorer, (e.dhome>0 AND e.dhome>=e.dvis) is_home
        FROM ev e WHERE e.dvis>0 OR e.dhome>0""")
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
        SELECT b.nid AS NFL_player_id, g.yr, g.wk, g.fr, COUNT(*) n_td
        FROM evt JOIN tg g ON evt.boxscore_id=g.boxscore_id AND evt.is_home=g.is_home
                 JOIN bio b ON b.pfr_id=evt.scorer
        WHERE b.nid IS NOT NULL GROUP BY 1,2,3,4""")
    return con.execute("SELECT COUNT(*), COALESCE(SUM(n_td),0) FROM tgt").fetchone()


def _agree(con, tbl):
    """pre-1978 team-game def_int_ret_td (v26 IDP sum) vs scoring count."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scw AS
        SELECT g.yr, g.wk, g.fr, COUNT(*) sc FROM evt
        JOIN tg g ON evt.boxscore_id=g.boxscore_id AND evt.is_home=g.is_home GROUP BY 1,2,3""")
    r = con.execute(f"""
        WITH v AS (SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, nfl_franchise_number fr,
              SUM(COALESCE(def_int_ret_td,0)) vv FROM {tbl}
            WHERE position<>'DEF' AND year<={YEAR_HI} GROUP BY 1,2,3)
        SELECT ROUND(100.0*SUM(CASE WHEN abs(COALESCE(v.vv,0)-scw.sc)<0.5 THEN 1 ELSE 0 END)/COUNT(*),1)
        FROM scw LEFT JOIN v USING(yr,wk,fr)""").fetchone()
    return r[0]


def apply_build():
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    _build_target(con)
    pre = _agree(con, "st")
    # single-row player-weeks only (skip doubleheaders); IDP rows only
    con.execute(f"""CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st
        WHERE year<={YEAR_HI} AND position<>'DEF' GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    nm = ("NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id "
          "AND m.year=st.year AND m.week=st.week)")
    updated = con.execute(f"""SELECT COUNT(*) FROM st JOIN tgt t
        ON st.NFL_player_id=t.NFL_player_id AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
        WHERE st.position<>'DEF' AND COALESCE(st.def_int_ret_td,0) < t.n_td AND {nm}""").fetchone()[0]
    con.execute(f"""UPDATE st SET def_int_ret_td=t.n_td,
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.NFL_player_id=t.NFL_player_id AND CAST(st.year AS INT)=t.yr
          AND CAST(st.week AS INT)=t.wk AND st.position<>'DEF' AND COALESCE(st.def_int_ret_td,0)<t.n_td AND {nm}""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    post = _agree(con, "st")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_irttmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and (post >= pre) and (updated > 0)
    res = {"updated": int(updated), "before": int(before), "after": int(after),
           "pre_agree": pre, "post_agree": post, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preirt_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_build()
        print(f"updated={r['updated']} rows {r['before']:,}->{r['after']:,} | "
              f"pre-1978 def_int_ret_td agree {r['pre_agree']}->{r['post_agree']}% | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        con.execute(f"CREATE TABLE st AS SELECT * FROM '{v}'")
        ngrp, ntd = _build_target(con)
        print(f"scoring INT-ret-TD events pre-1978 mapped to a player-week: {ngrp} ({ntd:.0f} TDs)")
        print(f"current pre-1978 def_int_ret_td team-game agree vs scoring: {_agree(con,'st')}%")
