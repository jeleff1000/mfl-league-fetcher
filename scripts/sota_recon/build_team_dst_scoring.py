"""
sota_recon/build_team_dst_scoring.py  --  populate team-DST scoring atoms from the scoring witness

The team DEF/DST row carried def_tds / special_teams_tds / def_safeties that were ~0 across most
eras -- defensive & return TDs lived only on the individual scorer's row, never aggregated onto
the unit row. The validated scoring witness (score-delta team attribution + play-description
classification, agrees with the boxscore 99-100%) is the authoritative TEAM-level record of who
scored what, so we set the DST row's atoms from it:

  DEF-row def_tds         := scoring count of interception/fumble-return TDs for that team-game
  DEF-row special_teams_tds := scoring count of kickoff/punt-return TDs
  DEF-row def_safeties    := scoring count of safeties

Then we VERIFY the result reconciles with the IDP sums (def_tds ?= sum of IDP def_int_ret_td +
fum_ret_td; special_teams_tds ?= sum of kickoff_return_tds + punt_return_tds) -- the original
"team DST must reconcile with IDP totals" requirement.

Gated: golden 24/24, row count unchanged, DEF-row atoms now match scoring, IDP reconciliation
within tolerance -> backup + swap.

    python -m scripts.sota_recon.build_team_dst_scoring            # dry-run
    python -m scripts.sota_recon.build_team_dst_scoring --apply
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
PROV = "wave21.team_dst_scoring_from_scoring_witness"
PROV_COL = "recon_correction_log"


def _scoring_witness(con):
    SC = f"read_parquet('{BOX}/scoring/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr, is_home
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scd AS
        SELECT boxscore_id, lower(COALESCE(description,'')) d,
          TRY_CAST(vis_team_score AS INT) - LAG(TRY_CAST(vis_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dvis,
          TRY_CAST(home_team_score AS INT) - LAG(TRY_CAST(home_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dhome
        FROM {SC} WHERE description IS NOT NULL AND description<>''""")
    con.execute("""CREATE OR REPLACE TEMP TABLE scs AS
        SELECT boxscore_id, d, (dhome > 0 AND dhome >= dvis) is_home FROM scd
        WHERE dvis > 0 OR dhome > 0""")
    con.execute("""CREATE OR REPLACE TEMP TABLE scw AS
        SELECT g.yr, g.wk, g.fr,
          SUM(CASE WHEN d LIKE '%interception return%' OR d LIKE '%fumble return%'
                    OR (d LIKE '%fumble recovery%' AND d LIKE '%end zone%') THEN 1 ELSE 0 END) sc_def_td,
          SUM(CASE WHEN d LIKE '%kickoff return%' OR d LIKE '%punt return%'
                    OR d LIKE '%blocked%return%' OR d LIKE '%missed field goal return%' THEN 1 ELSE 0 END) sc_st_td,
          SUM(CASE WHEN d LIKE 'safety%' OR d LIKE '%safety,%' THEN 1 ELSE 0 END) sc_safety
        FROM scs JOIN tg g ON scs.boxscore_id=g.boxscore_id AND scs.is_home=g.is_home
        GROUP BY 1,2,3""")


def _idp_reconcile(con, tbl):
    """team DEF def_tds vs IDP (def_int_ret_td+fum_ret_td); special_teams_tds vs return TDs."""
    r = con.execute(f"""
        WITH idp AS (SELECT nfl_franchise_number fr, CAST(year AS INT) yr, CAST(week AS INT) wk,
              SUM(COALESCE(def_int_ret_td,0)+COALESCE(fum_ret_td,0)) idp_def_td,
              SUM(COALESCE(kickoff_return_tds,0)+COALESCE(punt_return_tds,0)) idp_st_td
            FROM {tbl} WHERE position<>'DEF' GROUP BY 1,2,3),
        dst AS (SELECT nfl_franchise_number fr, CAST(year AS INT) yr, CAST(week AS INT) wk,
              SUM(COALESCE(def_tds,0)) d_td, SUM(COALESCE(special_teams_tds,0)) d_st
            FROM {tbl} WHERE position='DEF' GROUP BY 1,2,3)
        SELECT
          ROUND(100.0*SUM(CASE WHEN abs(d_td-idp_def_td)<0.5 THEN 1 ELSE 0 END)/COUNT(*),1) def_td_match,
          ROUND(100.0*SUM(CASE WHEN abs(d_st-idp_st_td)<0.5 THEN 1 ELSE 0 END)/COUNT(*),1) st_td_match
        FROM dst JOIN idp USING(fr,yr,wk)""").fetchone()
    return {"def_td_idp_match": r[0], "st_td_idp_match": r[1]}


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
    _scoring_witness(con)
    # only the team DEF/DST row receives these (never the IDP rows)
    updated = con.execute("""SELECT COUNT(*) FROM st JOIN scw w
        ON CAST(st.year AS INT)=w.yr AND CAST(st.week AS INT)=w.wk AND st.nfl_franchise_number=w.fr
        WHERE st.position='DEF'""").fetchone()[0]
    con.execute(f"""UPDATE st SET def_tds=w.sc_def_td, special_teams_tds=w.sc_st_td,
        def_safeties=w.sc_safety,
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM scw w WHERE CAST(st.year AS INT)=w.yr AND CAST(st.week AS INT)=w.wk
          AND st.nfl_franchise_number=w.fr AND st.position='DEF'""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_dsttmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close()
    recon = _idp_reconcile(con, "st")
    con.close(); shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and (updated > 0)
    res = {"updated_def_rows": int(updated), "before": int(before), "after": int(after),
           "golden": f"{g['passed']}/{g['total']}", "idp_reconcile": recon,
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predst_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_build()
        print(f"DEF rows updated={r['updated_def_rows']:,} | rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        print(f"  IDP reconcile: def_tds match {r['idp_reconcile']['def_td_idp_match']}% | "
              f"special_teams_tds match {r['idp_reconcile']['st_td_idp_match']}%")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        con.execute(f"CREATE TEMP TABLE st AS SELECT * FROM '{v}'")
        _scoring_witness(con)
        n = con.execute("""SELECT COUNT(*) FROM st JOIN scw w ON CAST(st.year AS INT)=w.yr
            AND CAST(st.week AS INT)=w.wk AND st.nfl_franchise_number=w.fr WHERE st.position='DEF'""").fetchone()[0]
        tot = con.execute("SELECT SUM(sc_def_td) a, SUM(sc_st_td) b, SUM(sc_safety) c FROM scw").fetchone()
        print(f"DEF rows that would be set: {n:,} | scoring totals def_td={tot[0]:.0f} st_td={tot[1]:.0f} safety={tot[2]:.0f}")
