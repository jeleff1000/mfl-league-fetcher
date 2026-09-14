"""
sota_recon/build_synthetic_resolution.py  --  resolve synthetic player ids into real ones

Synthetic ids (HIST-*, SYN-ANCIENT-*, [A-Z]{2,4}\\d{5,7}) are the root cause of the recurring
duplicate-player splits: a player's stats live under a synthetic id from one source and under
a real id (pfr/gsis) from another, so backfills create two rows for one person (e.g. Andy
Tomasic 1942: offense under HIST-99595679 / defense under TomaAn20).

Census (find-them-all): 5,784 synthetic ids. Of those:
  76    co-occur with a real id in the SAME (franchise, year, week, name) -> definite same-
        person split. THIS lane merges those (the active duplicates).
  413   name-match a real id elsewhere (handled separately, with same-name-different-person care)
  5,371 standalone (no real id with that name) -> genuinely-unidentified real players, NOT dups.

Mechanic: remap synthetic id -> its co-occurring real id; then for the (real_id, year, week,
season_type) rows that collapse, MAX-combine the NON-DERIVED stats (offense row + defense row
are disjoint, so MAX merges them into one true two-way-player row) and drop the extra; never
collapse genuine doubleheaders (distinct opponents). Remove the merged-away synthetic bio rows.
Gated: dup_person/splits down, golden holds -> backup + swap.

    python -m scripts.sota_recon.build_synthetic_resolution            # dry-run remap
    python -m scripts.sota_recon.build_synthetic_resolution --apply
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

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave15.synthetic_id_resolution"
PROV_COL = "recon_correction_log"
SYNTH = ("(NFL_player_id LIKE 'HIST-%' OR NFL_player_id LIKE 'SYN-%' "
         "OR regexp_matches(NFL_player_id,'^[A-Z]{2,4}[0-9]{5,7}$'))")
REAL = ("(regexp_matches(NFL_player_id,'^00-[0-9]+$') "
        "OR regexp_matches(NFL_player_id,'^[A-Za-z][A-Za-z.''-]*[0-9]{2}$'))")
_NN = "lower(regexp_replace(player,'[^A-Za-z]','','g'))"
# non-derived numeric stat columns are MAX-combined; identity/derived are left to one row
ID_COLS = {"NFL_player_id", "player", "year", "week", "season_type", "player_week",
           "nfl_team", "nfl_franchise_number", "opponent_nfl_franchise_number", "position",
           PROV_COL, "data_source"}


def _remap(con, V) -> int:
    """Build temp table remap(synth_id, real_id) from co-occurrence; return count."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE r2 AS
        SELECT NFL_player_id id, nfl_franchise_number fr, year, week, {_NN} nn,
               {SYNTH} is_s, {REAL} is_r FROM {V} WHERE player IS NOT NULL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE remap AS
        WITH pairs AS (
          SELECT DISTINCT s.id synth, r.id realid
          FROM r2 s JOIN r2 r ON r.fr=s.fr AND r.year=s.year AND r.week=s.week AND r.nn=s.nn
          WHERE s.is_s AND r.is_r)
        SELECT synth, ANY_VALUE(realid) realid FROM pairs
        GROUP BY synth HAVING COUNT(DISTINCT realid)=1""")   # unique real per synth only
    return con.execute("SELECT COUNT(*) FROM remap").fetchone()[0]


def apply_backfill() -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='5GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    types = {c[0]: c[1] for c in con.execute("DESCRIBE st").fetchall()}
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    n = _remap(con, "st")
    con.execute("DROP TABLE IF EXISTS r2")

    # 1) remap synthetic -> real id
    con.execute(f"""UPDATE st SET NFL_player_id=rm.realid,
        player_week=rm.realid||'_'||CAST(CAST(year AS BIGINT) AS VARCHAR)||'_'||CAST(CAST(week AS BIGINT) AS VARCHAR),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM remap rm WHERE st.NFL_player_id=rm.synth""")

    # 2) DOUBLEHEADER-AWARE collapse: only merge rows that are the SAME GAME (same opponent).
    #    Build a merge key per row touched by the remap:
    #      single-game week (<=1 distinct non-null opponent) -> merge by player-week
    #      doubleheader week (>=2 opponents)                 -> merge by (player-week, opponent)
    #      a NULL-opponent row inside a doubleheader week     -> SOLO key (never merges; these are
    #                                                            stray week-aggregate rows, e.g. Nevers 1929)
    num = [c for c, t in types.items() if c not in ID_COLS
           and any(k in t.upper() for k in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT"))]
    con.execute(f"""CREATE TEMP TABLE cand AS
        SELECT st.rowid rid, st.NFL_player_id id, st.year yr, st.week wk, st.season_type stt,
               st.opponent_nfl_franchise_number opp
        FROM st WHERE st.{PROV_COL} LIKE '%{PROV}%' OR st.NFL_player_id IN (SELECT realid FROM remap)""")
    con.execute("""CREATE TEMP TABLE pw AS
        SELECT id,yr,wk,stt, COUNT(DISTINCT opp) ndopp FROM cand WHERE opp IS NOT NULL GROUP BY 1,2,3,4""")
    con.execute("""CREATE TEMP TABLE mk AS
        SELECT c.rid,
          CASE WHEN COALESCE(p.ndopp,0)<=1 THEN c.id||'|'||c.yr||'|'||c.wk||'|'||c.stt
               WHEN c.opp IS NOT NULL THEN c.id||'|'||c.yr||'|'||c.wk||'|'||c.stt||'|'||CAST(c.opp AS VARCHAR)
               ELSE 'SOLO|'||CAST(c.rid AS VARCHAR) END mkey
        FROM cand c LEFT JOIN pw p ON p.id=c.id AND p.yr=c.yr AND p.wk=c.wk AND p.stt=c.stt""")
    con.execute("""CREATE TEMP TABLE grp AS
        SELECT mkey, MIN(rid) survivor, COUNT(*) c FROM mk GROUP BY mkey HAVING COUNT(*)>1""")
    con.execute(f"""CREATE TEMP TABLE combined AS
        SELECT m.mkey, {', '.join(f'MAX(st.{c}) {c}' for c in num)}
        FROM st JOIN mk m ON st.rowid=m.rid JOIN grp g ON m.mkey=g.mkey GROUP BY m.mkey""")
    setc = ", ".join(f"{c}=cb.{c}" for c in num)
    con.execute(f"""UPDATE st SET {setc} FROM combined cb JOIN grp g ON cb.mkey=g.mkey
        WHERE st.rowid=g.survivor""")
    dropped = con.execute("SELECT COALESCE(SUM(c-1),0) FROM grp").fetchone()[0]
    con.execute("""DELETE FROM st WHERE rowid IN
        (SELECT m.rid FROM mk m JOIN grp g ON m.mkey=g.mkey WHERE m.rid<>g.survivor)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
                    WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_syntmp.parquet")
    rdr = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema)
    for batch in rdr:
        w.write_batch(batch)
    w.close()
    remap_ids = [r[0] for r in con.execute("SELECT synth FROM remap").fetchall()]
    con.close(); shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    dp = duckdb.connect(); dp.execute("SET memory_limit='3GB'")
    syn_split = dp.execute(f"""SELECT COUNT(*) FROM (
        WITH r AS (SELECT NFL_player_id id, nfl_franchise_number fr, year, week,
                          lower(regexp_replace(player,'[^A-Za-z]','','g')) nn,
                          ({SYNTH.replace('NFL_player_id','NFL_player_id')}) s,
                          ({REAL.replace('NFL_player_id','NFL_player_id')}) rr
                   FROM read_parquet('{tmp}') WHERE player IS NOT NULL)
        SELECT 1 FROM r a WHERE a.s AND EXISTS (SELECT 1 FROM r b WHERE b.rr AND b.fr=a.fr
              AND b.year=a.year AND b.week=a.week AND b.nn=a.nn))""").fetchone()[0]
    dp.close()
    gate = (g["failed"] == 0) and (after <= before) and (syn_split == 0)
    res = {"synth_remapped": int(n), "before": int(before), "after": int(after),
           "rows_collapsed": int(before - after), "golden": f"{g['passed']}/{g['total']}",
           "synthetic_splits_remaining": int(syn_split),
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_presyn_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp)
        # drop merged-away synthetic bio rows
        bio = pq.read_table(BIO).to_pandas(); n0 = len(bio)
        bio = bio[~bio.NFL_player_id.isin(remap_ids)]
        import pyarrow as pa
        shutil.copy2(BIO, BIO.replace(".parquet", f"_backup_{stamp}.parquet"))
        pq.write_table(pa.Table.from_pandas(bio), BIO)
        res["backup"] = str(bk); res["swapped"] = True; res["bio"] = f"{n0}->{len(bio)}"
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_backfill()
        print(f"synthetic ids remapped: {r['synth_remapped']} | rows {r['before']:,}->{r['after']:,} "
              f"(collapsed {r['rows_collapsed']}) | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']}, bio {r['bio']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); d = duckdb.connect(); d.execute("SET memory_limit='4GB'")
        d.execute(f"CREATE TABLE st AS SELECT * FROM '{v}'")
        print("synthetic->real remap pairs:", _remap(d, "st"))
