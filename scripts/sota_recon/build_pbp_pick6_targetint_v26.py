"""
sota_recon/build_pbp_pick6_targetint_v26.py -- derive pick6 + receiving_target_interceptions from raw PBP.

Two defects found by the 2026-07-16 witness audit's double-entry mirror lane (runbook §5d), both
root-caused to builders that never ran over the v26 corpus:

1. `pick6` (+ pts_pick6 / pts_pick6_n1 / pts_pick6_n2) is **100% EMPTY in every era** while pbp_merged
   holds 2,181 pick-sixes 1978-2025. Root cause: build_nfl_super_table only fills `pts_pick6` (not the
   `pick6` atom), only for `position=='QB' AND year>=2016`, and only from loose nflverse files under
   `C:/Users/joeye/Downloads/play_by_play_*.parquet` -- a path outside the lake that the v26 build never
   used. The mirror `pick6(A) == def_int_ret_td(B)` therefore failed one-directionally in every decade.
2. `receiving_target_interceptions` has an exact **1999-2017 hole** (1978-98 filled by the PFR pbp text
   parse, 2018+ by PFR advanced charting; the nflverse-era middle was never computed).

Attribution (both unambiguous, no allocation): the passer of an interception returned for a TD owns the
pick6; the INTENDED receiver of an interception owns the target-interception. id crosswalk = pfr id
('pfr:AndeJo20') -> player_bio.pfr_id -> NFL_player_id, COALESCE-fallback to the raw pid for players whose
super rows literally carry the pfr-format id (the Henry Thomas class). Modern gsis ids pass through.

SCOPE / INVARIANCE: `pick6` feeds ONLY the opt-in pts_pick6* components (a league multiplier for
pick-sixes thrown); `receiving_target_interceptions` feeds no scoring column. Neither is in the base
offense recipe (offense_recipe.EXPECTED_4PT_HALF) nor in build_idp_scoring's atom set -> **fpts_* is
invariant and no rescore cascade is required** (gated below). pts_pick6* ARE recomputed here, in lockstep
with the atom, so the surface stays self-consistent.

    python -m scripts.sota_recon.build_pbp_pick6_targetint_v26            # dry-run
    python -m scripts.sota_recon.build_pbp_pick6_targetint_v26 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import latest_v26

PBP = "D:/league-history-data/nfl/raw/stathead/generated/pbp_merged_1978_2025/**/*.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave58.pbp_pick6_targetint"

# --- receiving_target_interceptions seam scope -------------------------------------------------
# The COLUMN hole is 1999-2017 (1998 populated by the pbp text parse, 2018+ by PFR advanced charting).
# But PBP can only close PART of it. Measured INT-receiver attribution in pbp_merged (share of
# interception plays that name the intended receiver):
#     1999 77.1% | 2000 91.2% | 2001 89.7% | 2002 89.2% |
#     2003 0.5% | 2004 0.2% | 2005 0.0% | 2006 0.0% | 2007 0.0% | 2008 0.0%   <-- BLACK HOLE
#     2009 97.6% | 2010 97.9% | 2011-2017 99.0-99.4%
# So the fill is scoped to 2009-2017, where the witness is essentially COMPLETE and therefore both the
# events AND the "targeted but never picked = 0" default are true statements.
# 1999-2008 is deliberately LEFT NULL (unknown), NOT zero: with 0-0.5% attribution in 2003-08 a zero would
# assert "this receiver was never targeted on an INT" for ~3,200 interceptions whose target PBP never
# recorded. (A first cut of this builder DID stamp 39,093 such false zeros -- caught by the
# passing_interceptions==receiving_target_interceptions mirror reading 77.9% vs the 98.7% PFR-charted
# control, and reverted. See runbook 9.2.) Closing 1999-2008 needs a different witness (NFL.com logs /
# PFR charting), tracked as a documented gap.
SEAM_LO, SEAM_HI = 2009, 2017


def _build_derived(con) -> None:
    """player-week credits: pick6 by passer (1978+), target-INT by intended receiver (seam years)."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE p6_raw AS
        SELECT CAST(season AS INT) yr, CAST(week AS INT) wk,
               REPLACE(CAST(passer_player_id AS VARCHAR), 'pfr:', '') pid,
               COUNT(*) p6
        FROM read_parquet('{PBP}', union_by_name=true)
        WHERE interception = 1 AND return_touchdown = 1
          AND passer_player_id IS NOT NULL AND CAST(passer_player_id AS VARCHAR) <> ''
        GROUP BY 1, 2, 3""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ti_raw AS
        SELECT CAST(season AS INT) yr, CAST(week AS INT) wk,
               REPLACE(CAST(receiver_player_id AS VARCHAR), 'pfr:', '') pid,
               COUNT(*) ti
        FROM read_parquet('{PBP}', union_by_name=true)
        WHERE interception = 1
          AND season BETWEEN {SEAM_LO} AND {SEAM_HI}
          AND receiver_player_id IS NOT NULL AND CAST(receiver_player_id AS VARCHAR) <> ''
        GROUP BY 1, 2, 3""")
    xw = (f"LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}') "
          f"WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) b ON b.pfr_id = r.pid")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE p6 AS
        SELECT COALESCE(b.NFL_player_id, r.pid) nfl_id, r.yr, r.wk, SUM(r.p6) p6
        FROM p6_raw r {xw} GROUP BY 1, 2, 3""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ti AS
        SELECT COALESCE(b.NFL_player_id, r.pid) nfl_id, r.yr, r.wk, SUM(r.ti) ti
        FROM ti_raw r {xw} GROUP BY 1, 2, 3""")


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='3GB'")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")

    _build_derived(con)
    p6_events = con.execute("SELECT COALESCE(SUM(p6),0) FROM p6_raw").fetchone()[0]
    ti_events = con.execute("SELECT COALESCE(SUM(ti),0) FROM ti_raw").fetchone()[0]
    p6_pids = con.execute("SELECT COUNT(DISTINCT pid) FROM p6_raw").fetchone()[0]
    p6_matched = con.execute(
        f"SELECT COUNT(DISTINCT r.pid) FROM p6_raw r JOIN (SELECT DISTINCT pfr_id FROM read_parquet('{BIO}') "
        f"WHERE pfr_id IS NOT NULL) b ON b.pfr_id=r.pid").fetchone()[0]
    if not apply:
        shutil.rmtree(sp, ignore_errors=True)
        return {"pick6_events_pbp": int(p6_events), "target_int_events_seam": int(ti_events),
                "pick6_distinct_pids": p6_pids, "pick6_pids_via_bio_pfr": p6_matched,
                "note": "remaining pids resolve via COALESCE passthrough (modern gsis ids)"}

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    b_p6 = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(pick6 AS DOUBLE),0)),0) FROM st").fetchone()[0]
    b_ti = con.execute(f"SELECT ROUND(SUM(COALESCE(TRY_CAST(receiving_target_interceptions AS DOUBLE),0)),0) "
                       f"FROM st WHERE year BETWEEN {SEAM_LO} AND {SEAM_HI}").fetchone()[0]

    # 1) pick6 atom: PBP is the sole witness and the column is empty -> set where PBP has the QB-week.
    con.execute("""UPDATE st SET pick6 = p.p6
        FROM p6 p WHERE st.position<>'DEF'
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(p.nfl_id AS VARCHAR)
          AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk""")
    # QBs who threw INTs in a PBP-covered year with no pick-6 -> a TRUE zero (not unknown)
    con.execute("""UPDATE st SET pick6 = 0
        WHERE pick6 IS NULL AND position<>'DEF' AND CAST(year AS INT) BETWEEN 1978 AND 2025
          AND COALESCE(TRY_CAST(passing_interceptions AS DOUBLE),0) > 0""")
    # 2) pts_pick6 family in lockstep with the atom (pure identities over pick6)
    con.execute("""UPDATE st SET pts_pick6 = COALESCE(TRY_CAST(pick6 AS DOUBLE),0),
                                 pts_pick6_n1 = COALESCE(TRY_CAST(pick6 AS DOUBLE),0) * -1,
                                 pts_pick6_n2 = COALESCE(TRY_CAST(pick6 AS DOUBLE),0) * -2
        WHERE position<>'DEF' AND pick6 IS NOT NULL""")
    # 3) receiving_target_interceptions: fill the measured 1999-2017 hole only (1978-98 text parse and
    #    2018+ PFR charting are existing witnessed values -- not overwritten).
    con.execute(f"""UPDATE st SET receiving_target_interceptions = t.ti
        FROM ti t WHERE st.position<>'DEF'
          AND CAST(st.year AS INT) BETWEEN {SEAM_LO} AND {SEAM_HI}
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(t.nfl_id AS VARCHAR)
          AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk""")
    con.execute(f"""UPDATE st SET receiving_target_interceptions = 0
        WHERE receiving_target_interceptions IS NULL AND position<>'DEF'
          AND CAST(year AS INT) BETWEEN {SEAM_LO} AND {SEAM_HI}
          AND COALESCE(TRY_CAST(targets AS DOUBLE),0) > 0""")
    con.execute(f"""UPDATE st SET recon_correction_log =
            CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''
                 THEN '{PROV}' ELSE recon_correction_log || ';{PROV}' END
        WHERE position<>'DEF' AND CAST(year AS INT)>=1978
          AND (COALESCE(TRY_CAST(pick6 AS DOUBLE),0)>0
               OR (CAST(year AS INT) BETWEEN {SEAM_LO} AND {SEAM_HI}
                   AND COALESCE(TRY_CAST(receiving_target_interceptions AS DOUBLE),0)>0))""")

    a_p6 = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(pick6 AS DOUBLE),0)),0) FROM st").fetchone()[0]
    a_ti = con.execute(f"SELECT ROUND(SUM(COALESCE(TRY_CAST(receiving_target_interceptions AS DOUBLE),0)),0) "
                       f"FROM st WHERE year BETWEEN {SEAM_LO} AND {SEAM_HI}").fetchone()[0]

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_p6ti.parquet")
    rb = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    _f = "COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)"
    fpts_orig = con.execute(f"SELECT ROUND(SUM({_f}),1) FROM read_parquet('{Path(v26).as_posix()}')").fetchone()[0]
    fpts_new = con.execute(f"SELECT ROUND(SUM({_f}),1) FROM read_parquet('{tq}')").fetchone()[0]
    con.close()

    gate = (after_rows == before_rows and fpts_orig == fpts_new and a_p6 > 0 and a_ti > 0)
    res = {"pick6_total": f"{b_p6} -> {a_p6}", "pick6_events_pbp": int(p6_events),
           "target_int_seam_total": f"{b_ti} -> {a_ti}", "target_int_events_pbp": int(ti_events),
           "rows": f"{before_rows} -> {after_rows}", "fpts_invariant": fpts_orig == fpts_new,
           "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_prep6ti_{stamp}.parquet"); shutil.copy2(vp, backup)
        os.replace(tmp, vp); res["backup"] = backup.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    print(run(apply=a.apply))
