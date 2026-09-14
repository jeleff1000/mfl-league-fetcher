"""
sota_recon/build_pbp_idp_backfill_v26.py -- derive individual TACKLES + SACKS from raw PBP (1978+).

Joe 2026-07-16: individual tackles/sacks are PBP-derivable back to 1978 (pbp_merged has the per-play
credit columns). v26's individual solo/assist tackles are sparse pre-1999 (official participation floor)
and individual sacks start 1982 (official). PBP fills BOTH further back and more authoritatively:
  * tackles  -> fill 1978-1998 (v26 sparse; 1999+ already official, left untouched)
  * sacks    -> fill 1978-1981 (v26 official from 1982; extend back to 1978)

id crosswalk: PBP 1978-98 uses pfr ids ('pfr:AndeJo20'); super uses NFL_player_id (legacy/gsis). Map via
player_bio.pfr_id -> NFL_player_id. INTs/FRs/def-TDs/safeties already carry individual data -> NOT touched.

Points: tackles/sacks feed pts_idp_std, NOT fpts -> golden (fpts) is invariant here (gated). Re-run
build_idp_scoring_v26 AFTER this to propagate into pts_idp_std.

    python -m scripts.sota_recon.build_pbp_idp_backfill_v26 [--apply]
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

PBP = "D:/league-history-data/nfl/raw/stathead/generated/pbp_merged_1978_2025/**/*.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"

SOLO = ["solo_tackle_1_player_id", "solo_tackle_2_player_id"]
ASST = ["assist_tackle_1_player_id", "assist_tackle_2_player_id",
        "assist_tackle_3_player_id", "assist_tackle_4_player_id"]
SACK_FULL = ["sack_player_id"]
SACK_HALF = ["half_sack_1_player_id", "half_sack_2_player_id"]


def _credit_union(cols, solo, asst, sk) -> str:
    parts = []
    for c in cols:
        parts.append(
            f"SELECT CAST(season AS INT) AS yr, CAST(week AS INT) AS wk, "
            f"REPLACE(CAST({c} AS VARCHAR), 'pfr:', '') pid, "
            f"{solo} solo, {asst} asst, {sk} sk "
            f"FROM read_parquet('{PBP}', union_by_name=true) "
            f"WHERE season BETWEEN 1978 AND 1998 AND {c} IS NOT NULL AND CAST({c} AS VARCHAR) <> ''"
        )
    return "\nUNION ALL\n".join(parts)


def _build_derived(con) -> None:
    union = "\nUNION ALL\n".join([
        _credit_union(SOLO, 1.0, 0.0, 0.0),
        _credit_union(ASST, 0.0, 1.0, 0.0),
        _credit_union(SACK_FULL, 0.0, 0.0, 1.0),
        _credit_union(SACK_HALF, 0.0, 0.0, 0.5),
    ])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbp_credits AS
        SELECT pid, yr, wk, SUM(solo) solo, SUM(asst) asst, SUM(sk) sacks
        FROM ({union}) GROUP BY pid, yr, wk""")
    # crosswalk pfr id -> NFL_player_id via bio.pfr_id; FALL BACK to c.pid itself for players whose bio
    # row stores the pfr id as an alias NFL_player_id (pfr_id NULL) -- their super rows literally use the
    # pfr-format id (e.g. Henry Thomas 'ThomHe00'). COALESCE catches those; a truly-absent pid falls back
    # to c.pid and simply matches no super row (harmless). NOTE: several of these carry DUPLICATE gsis-id
    # rows (super-table id split) -> logged to the dedup queue, resolved separately.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbp_derived AS
        SELECT COALESCE(b.NFL_player_id, c.pid) nfl_id, c.yr, c.wk, c.solo, c.asst, c.sacks
        FROM pbp_credits c
        LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}')
                   WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) b
          ON b.pfr_id = c.pid""")


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
    credits = con.execute("SELECT COUNT(*) FROM pbp_credits").fetchone()[0]
    matched = con.execute("SELECT COUNT(*) FROM pbp_derived").fetchone()[0]
    distinct_pids = con.execute("SELECT COUNT(DISTINCT pid) FROM pbp_credits").fetchone()[0]
    matched_pids = con.execute(
        f"SELECT COUNT(DISTINCT c.pid) FROM pbp_credits c "
        f"JOIN (SELECT DISTINCT pfr_id FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL) b "
        f"ON b.pfr_id=c.pid").fetchone()[0]
    if not apply:
        shutil.rmtree(sp, ignore_errors=True)
        return {"pbp_credit_rows": credits, "crosswalk_matched_rows": matched,
                "distinct_pfr_ids": distinct_pids, "matched_pfr_ids": matched_pids,
                "unmatched_pfr_ids": distinct_pids - matched_pids,
                "match_pct": round(100 * matched_pids / distinct_pids, 1) if distinct_pids else 0}

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')")
    before_solo = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(def_tackles_solo AS DOUBLE),0)),0) FROM st WHERE year BETWEEN 1978 AND 1998 AND position<>'DEF'").fetchone()[0]
    before_sack = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(def_sacks AS DOUBLE),0)),0) FROM st WHERE year BETWEEN 1978 AND 1981 AND position<>'DEF'").fetchone()[0]
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]

    # tackles: fill 1978-1998 where PBP has the (player, game). combined = solo + assist.
    tk = con.execute("""UPDATE st SET
            def_tackles_solo = p.solo,
            def_tackle_assists = p.asst,
            def_tackles_combined = p.solo + p.asst
        FROM pbp_derived p
        WHERE st.position<>'DEF' AND st.year BETWEEN 1978 AND 1998
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(p.nfl_id AS VARCHAR)
          AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk""").fetchone()
    # sacks: extend individual sacks back 1978-1981 (v26 official from 1982)
    sk = con.execute("""UPDATE st SET def_sacks = p.sacks
        FROM pbp_derived p
        WHERE st.position<>'DEF' AND st.year BETWEEN 1978 AND 1981 AND p.sacks>0
          AND CAST(st.NFL_player_id AS VARCHAR)=CAST(p.nfl_id AS VARCHAR)
          AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk""").fetchone()

    after_solo = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(def_tackles_solo AS DOUBLE),0)),0) FROM st WHERE year BETWEEN 1978 AND 1998 AND position<>'DEF'").fetchone()[0]
    after_sack = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(def_sacks AS DOUBLE),0)),0) FROM st WHERE year BETWEEN 1978 AND 1981 AND position<>'DEF'").fetchone()[0]

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_pbpidp.parquet")
    rb = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    # This step touches ONLY def tackles/sacks -> offense fpts must be invariant. pts_idp_std is
    # intentionally left stale here and recomputed by build_idp_scoring next, so DON'T gate on golden's
    # pts_idp check (it would fail on this intermediate state). Gate: rows unchanged + fpts invariant +
    # tackles actually increased. build_idp_scoring runs the full golden gate on the final state.
    _f = "COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)"
    fpts_orig = con.execute(f"SELECT ROUND(SUM({_f}),1) FROM read_parquet('{Path(v26).as_posix()}')").fetchone()[0]
    fpts_new = con.execute(f"SELECT ROUND(SUM({_f}),1) FROM read_parquet('{tq}')").fetchone()[0]
    con.close()
    gate = (after_rows == before_rows and after_solo > before_solo and fpts_orig == fpts_new)
    res = {"pbp_credit_rows": credits, "crosswalk_matched_rows": matched,
           "solo_1978_98": f"{before_solo} -> {after_solo}", "sacks_1978_81": f"{before_sack} -> {after_sack}",
           "fpts_invariant": fpts_orig == fpts_new, "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_prepbpidp_{stamp}.parquet"); shutil.copy2(vp, backup)
        os.replace(tmp, vp); res["backup"] = backup.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    print(run(apply=a.apply))
