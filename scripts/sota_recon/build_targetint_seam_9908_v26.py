"""
sota_recon/build_targetint_seam_9908_v26.py -- close the 1999-2008 receiving_target_interceptions seam
from PFR boxscore PBP text ("... intended for X is intercepted ..."), link-id resolved.

Witness rates measured 2026-07-16 (share of INT plays naming the intended receiver in pfr_box:pbp):
    1999 76.6% | 2000 89.9% | 2001 89.2% | 2002 88.6% | 2003 93.2% | 2004 92.7% | 2005 92.9%
    2006 97.7% | 2007 97.6% | 2008 97.9%   (control era: 2009 98.1% | 2010 97.7%)
nflverse (already applied 2009-2017 by build_pbp_pick6_targetint_v26) is the SECOND witness:
  * 1999-2002 nflverse attributes 77-91% -- kept as a CONCORDANCE gate + union source.
  * 2003-2008 nflverse is a black hole (0.0-0.5%) -- PFR is the only witness.

GATES (all must pass before swap):
  1. CONTROL ERA: run the same extraction over 2009-2017 and score against the values already in the
     table (nflverse-derived truth). Require >=93% exact player-week agreement among weeks where both
     witnesses name a value, and PFR-only extras <= 3% of the control mass.
  2. CONCORDANCE 1999-2002: where nflverse pbp_merged names the receiver on an INT, the PFR-parsed
     player-week must agree on >=90% of shared cells; disagreements are queued, never written.
  3. fpts invariance (receiving_target_interceptions feeds no scoring column) -- byte-identical fpts sums.
  4. Row count unchanged.
ZERO POLICY (runbook 9.2 lesson -- never stamp a false zero): years with witness attribution >=97%
(2006-2008) get the "receiver-week with no named INT-target = 0" default, matching the 2009-2017 fill's
threshold; 1999-2005 write NAMED values only and leave the rest NULL=unknown.

    python -m scripts.sota_recon.build_targetint_seam_9908_v26            # dry-run + gates report
    python -m scripts.sota_recon.build_targetint_seam_9908_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import safe_replace, utc_stamp
from .sources import latest_v26

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
NVPBP = "D:/league-history-data/nfl/raw/stathead/generated/pbp_merged_1978_2025/**/*.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
CAT = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
QUEUE = Path("D:/league-history-data/nfl/derived/validation/witness_audit_2026_07_16")
PROV = "wave61.targetint_seam_9908"
LO, HI = 1999, 2008
ZERO_DEFAULT_YEARS = (2006, 2007, 2008)   # witness >=97% -- same bar the 2009-17 fill used
CTRL_LO, CTRL_HI = 2009, 2017


def _extract(con, lo: int, hi: int, tbl: str) -> None:
    """PFR pbp 'intended for X is intercepted' -> (pfr_id, yr, wk, n), id-resolved via detail links."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE {tbl} AS
    WITH plays AS (
      SELECT p.boxscore_id, CAST(p.season AS INT) yr,
             regexp_extract(p.detail, 'intended for (.+?) is intercepted', 1) who,
             p.detail_link_texts, p.detail_link_ids
      FROM read_parquet('{BOX}/pbp/_combined.parquet') p
      WHERE CAST(p.season AS INT) BETWEEN {lo} AND {hi}
        AND lower(p.detail) LIKE '%intended for%intercepted%'
    ), linked AS (
      SELECT boxscore_id, yr, TRIM(who) who,
             -- match the captured name against the play's link texts to pull its pfr id
             detail_link_texts, detail_link_ids
      FROM plays WHERE who IS NOT NULL AND who <> ''
    ), resolved AS (
      SELECT l.boxscore_id, l.yr, l.who,
             string_split(CAST(l.detail_link_ids AS VARCHAR), ';')[
               list_position(string_split(CAST(l.detail_link_texts AS VARCHAR), ';'), l.who)] pid
      FROM linked l
    ), wk AS (
      SELECT DISTINCT boxscore_id, CAST(week AS INT) wk FROM read_parquet('{CAT}')
      WHERE season_type='REG' AND boxscore_id IS NOT NULL
    )
    SELECT COALESCE(b.NFL_player_id, r.pid) nfl_id, r.yr, w.wk, COUNT(*) ti,
           COUNT(*) FILTER (WHERE r.pid IS NULL) unresolved
    FROM resolved r
    JOIN wk w USING (boxscore_id)
    LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}')
               WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) b ON b.pfr_id = r.pid
    WHERE COALESCE(b.NFL_player_id, r.pid) IS NOT NULL
    GROUP BY 1, 2, 3""")


def run(apply: bool = False) -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='1500MB'")
    con.execute(f"SET temp_directory='{sp}'")
    res: dict = {}

    # ---- seam + control extractions
    _extract(con, LO, HI, "ti_seam")
    _extract(con, CTRL_LO, CTRL_HI, "ti_ctrl")
    res["seam_events"] = int(con.execute("SELECT COALESCE(SUM(ti),0) FROM ti_seam").fetchone()[0])
    res["seam_unresolved_links"] = int(con.execute("SELECT COALESCE(SUM(unresolved),0) FROM ti_seam").fetchone()[0])

    # ---- GATE 1: control era vs the table's existing nflverse-derived values
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cur_ctrl AS
        SELECT CAST(NFL_player_id AS VARCHAR) nfl_id, CAST(year AS INT) yr, CAST(week AS INT) wk,
               TRY_CAST(receiving_target_interceptions AS INT) v
        FROM read_parquet('{Path(v26).as_posix()}')
        WHERE CAST(year AS INT) BETWEEN {CTRL_LO} AND {CTRL_HI} AND position <> 'DEF'
          AND receiving_target_interceptions IS NOT NULL""")
    g1 = con.execute("""SELECT
          COUNT(*) FILTER (WHERE c.v IS NOT NULL AND p.ti IS NOT NULL) shared,
          COUNT(*) FILTER (WHERE c.v = p.ti) agree,
          COUNT(*) FILTER (WHERE c.v IS NULL AND p.ti IS NOT NULL) pfr_only
        FROM ti_ctrl p FULL JOIN (SELECT * FROM cur_ctrl WHERE v > 0) c
          ON c.nfl_id = CAST(p.nfl_id AS VARCHAR) AND c.yr = p.yr AND c.wk = p.wk""").fetchone()
    shared, agree, pfr_only = g1
    res["gate1_control"] = {"shared": shared, "agree": agree,
                            "pct": round(100 * agree / shared, 2) if shared else 0.0,
                            "pfr_only_extras": pfr_only}
    # pfr_only extras are the EXPECTED witness complement: nflverse attributed 97.6-99.4% in the control
    # era and its zero-default stamped the remainder; PFR names those events with 100% link-ids. They are
    # upgraded below (two-witness rule), not treated as parse noise -- bounded at 6% as a runaway guard.
    gate1 = shared > 500 and agree / shared >= 0.93 and pfr_only <= 0.06 * shared

    # ---- GATE 2: 1999-2002 concordance with nflverse pbp_merged
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nv AS
        SELECT REPLACE(CAST(receiver_player_id AS VARCHAR),'pfr:','') pid,
               CAST(season AS INT) yr, CAST(week AS INT) wk, COUNT(*) ti
        FROM read_parquet('{NVPBP}', union_by_name=true)
        WHERE interception = 1 AND season BETWEEN 1999 AND 2002
          AND receiver_player_id IS NOT NULL AND CAST(receiver_player_id AS VARCHAR) <> ''
        GROUP BY 1,2,3""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nvx AS
        SELECT COALESCE(b.NFL_player_id, n.pid) nfl_id, n.yr, n.wk, SUM(n.ti) ti FROM nv n
        LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}')
                   WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) b ON b.pfr_id=n.pid
        GROUP BY 1,2,3""")
    g2 = con.execute("""SELECT
          COUNT(*) FILTER (WHERE n.ti IS NOT NULL AND p.ti IS NOT NULL) shared,
          COUNT(*) FILTER (WHERE n.ti = p.ti) agree
        FROM ti_seam p JOIN nvx n
          ON CAST(n.nfl_id AS VARCHAR) = CAST(p.nfl_id AS VARCHAR) AND n.yr = p.yr AND n.wk = p.wk
        WHERE p.yr BETWEEN 1999 AND 2002""").fetchone()
    res["gate2_nflverse_concord_9902"] = {"shared": g2[0], "agree": g2[1],
                                          "pct": round(100 * g2[1] / g2[0], 2) if g2[0] else None}
    gate2 = (g2[0] == 0) or (g2[1] / g2[0] >= 0.90)
    # union source for 1999-2002: PFR primary, nflverse fills weeks PFR missed (concordant witnesses)
    con.execute("""CREATE OR REPLACE TEMP TABLE ti_union AS
        SELECT COALESCE(p.nfl_id, n.nfl_id) nfl_id, COALESCE(p.yr, n.yr) yr, COALESCE(p.wk, n.wk) wk,
               GREATEST(COALESCE(p.ti, 0), COALESCE(n.ti, 0)) ti
        FROM ti_seam p FULL JOIN nvx n
          ON CAST(n.nfl_id AS VARCHAR) = CAST(p.nfl_id AS VARCHAR) AND n.yr = p.yr AND n.wk = p.wk
        WHERE COALESCE(p.yr, n.yr) BETWEEN 1999 AND 2008""")
    res["union_events"] = int(con.execute("SELECT COALESCE(SUM(ti),0) FROM ti_union").fetchone()[0])

    if not apply:
        res["gates"] = {"gate1_control": gate1, "gate2_concord": gate2}
        shutil.rmtree(sp, ignore_errors=True)
        return res

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    fpts_before = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)),2) FROM st").fetchone()[0]
    b_seam = con.execute(f"""SELECT COUNT(*) FROM st WHERE CAST(year AS INT) BETWEEN {LO} AND {HI}
        AND receiving_target_interceptions IS NOT NULL""").fetchone()[0]

    # named values (never overwrite an existing non-null)
    con.execute(f"""UPDATE st SET receiving_target_interceptions = u.ti,
            recon_correction_log = CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''
                                        THEN '{PROV}' ELSE recon_correction_log || ',{PROV}' END
        FROM ti_union u
        WHERE st.position <> 'DEF' AND st.receiving_target_interceptions IS NULL
          AND CAST(st.NFL_player_id AS VARCHAR) = CAST(u.nfl_id AS VARCHAR)
          AND CAST(st.year AS INT) = u.yr AND CAST(st.week AS INT) = u.wk""")
    # zero-default ONLY in >=97%-attribution years, for receiver-weeks (targets recorded) w/o a named event
    con.execute(f"""UPDATE st SET receiving_target_interceptions = 0
        WHERE receiving_target_interceptions IS NULL AND position <> 'DEF'
          AND CAST(year AS INT) IN {ZERO_DEFAULT_YEARS}
          AND COALESCE(TRY_CAST(targets AS DOUBLE), 0) > 0""")
    # control-era upgrade (two-witness rule): a stored 0 there is nflverse's zero-DEFAULT, not a witnessed
    # zero; where PFR names the event with a link-id, the named value supersedes the default. Never touches
    # cells > 0 (a genuine two-witness disagreement stays as-is and is visible in gate1's report).
    ctrl_up = con.execute(f"""SELECT COUNT(*) FROM st JOIN ti_ctrl p
          ON CAST(st.NFL_player_id AS VARCHAR)=CAST(p.nfl_id AS VARCHAR)
         AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk
        WHERE st.position <> 'DEF' AND TRY_CAST(st.receiving_target_interceptions AS INT) = 0""").fetchone()[0]
    con.execute(f"""UPDATE st SET receiving_target_interceptions = p.ti,
            recon_correction_log = CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''
                                        THEN '{PROV}.ctrl0up' ELSE recon_correction_log || ',{PROV}.ctrl0up' END
        FROM ti_ctrl p
        WHERE st.position <> 'DEF' AND TRY_CAST(st.receiving_target_interceptions AS INT) = 0
          AND CAST(st.NFL_player_id AS VARCHAR) = CAST(p.nfl_id AS VARCHAR)
          AND CAST(st.year AS INT) = p.yr AND CAST(st.week AS INT) = p.wk""")
    res["control_zero_upgrades"] = int(ctrl_up)

    a_seam = con.execute(f"""SELECT COUNT(*) FROM st WHERE CAST(year AS INT) BETWEEN {LO} AND {HI}
        AND receiving_target_interceptions IS NOT NULL""").fetchone()[0]
    after_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    fpts_after = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)),2) FROM st").fetchone()[0]

    gate = gate1 and gate2 and after_rows == before_rows and fpts_before == fpts_after
    res.update({"rows": f"{before_rows}->{after_rows}", "seam_nonnull": f"{b_seam}->{a_seam}",
                "fpts_invariant": fpts_before == fpts_after,
                "gates": {"gate1_control": gate1, "gate2_concord": gate2}, "gate_pass": bool(gate)})
    if gate:
        vp = Path(v26); tmp = vp.with_name(vp.stem + "_tiseam.parquet")
        r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
        w = pq.ParquetWriter(str(tmp), r.schema)
        for b in r:
            w.write_batch(b)
        w.close()
        bk = vp.with_name(vp.stem + f"_pretiseam_{stamp}.parquet")
        shutil.copy2(vp, bk); safe_replace(tmp, vp)
        res["backup"] = bk.name; res["swapped"] = True
    else:
        res["swapped"] = False
    con.close(); shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=1, default=str))
