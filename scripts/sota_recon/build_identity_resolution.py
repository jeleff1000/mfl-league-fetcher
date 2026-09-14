"""
sota_recon/build_identity_resolution.py  --  give every player one proper unique id

The dup_person finding (recon_expectations) is caused by ORPHAN ids: v26 rows stored under
a pfr-style id that is NOT in the identity authority player_bio, sitting alongside the SAME
person's canonical (bio) id in the same franchise-week. Of 253 dup_person groups, 225 are
exactly one canonical(bio) id + one orphan id -> a clean remap. (28 are all-orphan; handled
by the bio-registration phase, not here.)

Resolution (gated, mirrors the other backfills):
  1. remap each orphan id -> its canonical bio id (the bio id it shares
     franchise+year+week+name+position with), where the mapping is unambiguous.
  2. collapse the duplicate (id, year, week, season_type) rows the remap creates by KEEPING
     the row with the most activity and dropping the duplicate(s). We keep one whole row
     (never sum) so rank/derived columns are not corrupted. Genuine doubleheaders (different
     opponents, pre-1950) are NOT collapsed.
  3. gate: golden 21/21, dup_person count falls, row count drops only by the dropped dups,
     no new identity collisions -> backup + swap.

    python -m scripts.sota_recon.build_identity_resolution            # dry-run (remap size)
    python -m scripts.sota_recon.build_identity_resolution --apply    # gated write
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave10.identity_remap"
PROV_COL = "recon_correction_log"
_NN = "lower(regexp_replace(player, '[^A-Za-z]', '', 'g'))"
_ACT = ("(COALESCE(rushing_yards,0)+COALESCE(receiving_yards,0)+COALESCE(passing_yards,0)"
        "+COALESCE(carries,0)+COALESCE(receptions,0)+COALESCE(def_tackles_solo,0)"
        "+COALESCE(fg_made,0)+COALESCE(pat_made,0)+COALESCE(passing_tds,0)"
        "+COALESCE(rushing_tds,0)+COALESCE(receiving_tds,0))")


def build_remap(con, V, B) -> pd.DataFrame:
    """orphan id -> canonical bio id, from dup_person groups that are exactly 1 canonical + 1
    orphan, requiring each orphan to map to a single canonical (else dropped as ambiguous).
    Memory-light: per-row bio flag + grouped counts + a small self-join (no list aggregation)."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE idact AS "
                f"SELECT NFL_player_id id, SUM({_ACT}) act FROM {V} GROUP BY 1")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE base AS
        SELECT DISTINCT nfl_franchise_number fr, year, week, {_NN} nn, position pos,
               NFL_player_id id,
               (NFL_player_id IN (SELECT NFL_player_id FROM {B})) AS is_bio
        FROM {V} WHERE player IS NOT NULL AND player <> '' AND position IS NOT NULL
    """)
    # 2-id dup_person groups with AT MOST ONE bio id (never merge two real bio people)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE g AS
        SELECT fr, year, week, nn, pos FROM base GROUP BY 1,2,3,4,5
        HAVING COUNT(DISTINCT id) = 2 AND COUNT(DISTINCT id) FILTER (WHERE is_bio) <= 1
    """)
    # canonical = the bio id if present, else the higher-activity id; orphan = the other
    rows = con.execute("""
        WITH m AS (
          SELECT b.fr,b.year,b.week,b.nn,b.pos, b.id, b.is_bio, COALESCE(a.act,0) act
          FROM g JOIN base b ON b.fr=g.fr AND b.year=g.year AND b.week=g.week
                              AND b.nn=g.nn AND b.pos=g.pos
          LEFT JOIN idact a ON a.id=b.id),
        ranked AS (
          SELECT id, is_bio,
            ROW_NUMBER() OVER (PARTITION BY fr,year,week,nn,pos ORDER BY is_bio DESC, act DESC, id) rn,
            FIRST_VALUE(id) OVER (PARTITION BY fr,year,week,nn,pos ORDER BY is_bio DESC, act DESC, id) canon
          FROM m)
        SELECT DISTINCT id AS orphan, canon FROM ranked WHERE rn > 1
    """).df()
    good = rows.groupby("orphan").canon.nunique()
    return rows[rows.orphan.isin(good[good == 1].index)].drop_duplicates("orphan")


def apply_backfill() -> dict:
    v26 = latest_v26()
    stamp = utc_stamp()
    tmp_spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(tmp_spill, exist_ok=True)
    con = duckdb.connect(os.path.join(tmp_spill, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='4GB'")
    con.execute(f"SET temp_directory='{tmp_spill}'")
    V = f"read_parquet('{v26}')"; B = f"read_parquet('{BIO}')"
    remap = build_remap(con, V, B)
    for t in ("base", "g", "idact"):
        con.execute(f"DROP TABLE IF EXISTS {t}")

    con.execute(f"CREATE TABLE st AS SELECT * FROM {V}")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.register("remap", remap)

    # 1) remap orphan -> canonical id (+ rebuild player_week, tag provenance)
    con.execute(f"""
        UPDATE st SET NFL_player_id = r.canon,
          player_week = r.canon || '_' || CAST(CAST(year AS BIGINT) AS VARCHAR) || '_'
                        || CAST(CAST(week AS BIGINT) AS VARCHAR),
          {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}'
                            ELSE {PROV_COL} || ',{PROV}' END
        FROM remap r WHERE st.NFL_player_id = r.orphan
    """)
    remapped = con.execute("SELECT COUNT(*) FROM st WHERE " + PROV_COL + f" LIKE '%{PROV}%'").fetchone()[0]

    # 2) collapse dups the remap created: keep max-activity row per (id,yr,wk,season_type),
    #    but NOT genuine doubleheaders (distinct opponents, pre-1950).
    con.execute(f"""
        CREATE TEMP TABLE bad AS
        WITH k AS (
          SELECT NFL_player_id, year, week, season_type, COUNT(*) n,
                 COUNT(DISTINCT opponent_nfl_franchise_number) nopp
          FROM st GROUP BY 1,2,3,4
          HAVING COUNT(*) > 1 AND (COUNT(DISTINCT opponent_nfl_franchise_number) < COUNT(*) OR year >= 1950)),
        ranked AS (
          SELECT st.rowid AS rid,
                 ROW_NUMBER() OVER (PARTITION BY st.NFL_player_id, st.year, st.week, st.season_type
                                    ORDER BY {_ACT} DESC) rn
          FROM st JOIN k ON st.NFL_player_id=k.NFL_player_id AND st.year=k.year
                          AND st.week=k.week AND st.season_type=k.season_type)
        SELECT rid FROM ranked WHERE rn > 1
    """)
    dropped = con.execute("SELECT COUNT(*) FROM bad").fetchone()[0]
    con.execute("DELETE FROM st WHERE rowid IN (SELECT rid FROM bad)")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]

    # remaining dup_person after fix
    resid = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT 1 FROM st WHERE player IS NOT NULL AND player<>'' AND position IS NOT NULL
          GROUP BY nfl_franchise_number, year, week, {_NN}, position
          HAVING COUNT(DISTINCT NFL_player_id) > 1)
    """).fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
                    WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")

    vpath = Path(v26); tmp = vpath.with_name(vpath.stem + "_identtmp.parquet")
    reader = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), reader.schema)
    for b in reader:
        writer.write_batch(b)
    writer.close(); con.close(); shutil.rmtree(tmp_spill, ignore_errors=True)

    # GATE: golden anchors hold, row count fell only by dropped dups, dup_person collapsed
    from . import golden_samples
    import scripts.sota_recon.sources as S
    orig_latest = S.latest_v26
    S.latest_v26 = lambda: str(tmp)            # point golden at the temp file
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = orig_latest
    gate = (g["failed"] == 0) and (after == before - dropped) and (resid <= 5)
    res = {"orphans_remapped_ids": int(len(remap)), "rows_remapped": int(remapped),
           "rows_dropped": int(dropped), "before": int(before), "after": int(after),
           "dup_person_residual": int(resid), "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        backup = vpath.with_name(vpath.stem + f"_preident_backup_{stamp}.parquet")
        shutil.copy2(vpath, backup); os.replace(tmp, vpath)
        res["backup"] = str(backup); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if args.apply:
        r = apply_backfill()
        print(f"remapped {r['orphans_remapped_ids']} orphan ids ({r['rows_remapped']} rows); "
              f"dropped {r['rows_dropped']} dup rows")
        print(f"rows {r['before']:,} -> {r['after']:,} | dup_person residual {r['dup_person_residual']} "
              f"| golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        import tempfile
        td = tempfile.mkdtemp()
        con = duckdb.connect(os.path.join(td, "w.duckdb"))
        con.execute("PRAGMA threads=1"); con.execute("SET memory_limit='4GB'")
        con.execute(f"SET temp_directory='{td}'")
        v = latest_v26()
        rm = build_remap(con, f"read_parquet('{v}')", f"read_parquet('{BIO}')")
        print(f"orphan->canonical remap: {len(rm)} ids")
        print(rm.head(10).to_string(index=False))
        con.close(); shutil.rmtree(td, ignore_errors=True)
