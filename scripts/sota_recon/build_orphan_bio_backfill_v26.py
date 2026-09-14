"""
sota_recon/build_orphan_bio_backfill_v26.py  --  anchor the 147 orphan stat-ids in player_bio.

recon_identity_ids found 147 super-table NFL_player_ids with NO player_bio row (mostly 1950-77 era
coverage + ~27 modern role players whose identity resolution fell back to a SYNTHETIC id like
'KaluN.20' that never matched bio). Remap to canonical bio ids is unsafe here: only 38 are unambiguous
name matches (72 map to >1 bio, 13 would merge). So anchor every orphan id 1:1 with a MINIMAL bio row
(NFL_player_id, player, nfl_position, first_year, last_year from the super table; all other bio cols
NULL) -- zero merge/false-match risk, clears the ORPHAN_STATS tripwire.

Writes the LOCAL ops_data/nfl_historical/player_bio.parquet (the identity anchor). Gate: bio rows +147,
no duplicate NFL_player_id, ORPHAN_STATS -> 0, original bio rows unchanged.

    python -m scripts.sota_recon.build_orphan_bio_backfill_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
DERIVED = {"NFL_player_id": "o.oid", "player": "o.nm", "nfl_position": "o.pos",
           "first_year": "o.y0", "last_year": "o.y1"}


def _orphan_select(con, biop, wk):
    desc = con.execute(f"DESCRIBE SELECT * FROM '{biop}'").fetchall()
    con.execute(f"""CREATE OR REPLACE TEMP TABLE orphan AS
      SELECT s.NFL_player_id oid, MAX(s.player) nm, MAX(s.nfl_position) pos,
             CAST(MIN(s.year) AS DOUBLE) y0, CAST(MAX(s.year) AS DOUBLE) y1
      FROM '{wk}' s LEFT JOIN '{biop}' b USING(NFL_player_id)
      WHERE b.NFL_player_id IS NULL AND s.NFL_player_id IS NOT NULL GROUP BY 1""")
    cols = []
    for name, typ, *_ in desc:
        if name in DERIVED:
            cols.append(f"CAST({DERIVED[name]} AS {typ}) AS {name}")
        else:
            cols.append(f"CAST(NULL AS {typ}) AS {name}")
    return f"SELECT {', '.join(cols)} FROM orphan o"


def run(apply=False):
    biop = Path(BIO).as_posix(); wk = Path(latest_v26()).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{biop}'").fetchone()[0]
    sel = _orphan_select(con, biop, wk)
    n_orphan = con.execute("SELECT COUNT(*) FROM orphan").fetchone()[0]
    if not apply:
        con.close(); return {"bio_rows": before, "orphans": n_orphan}

    out_sql = f"SELECT * FROM '{biop}' UNION ALL BY NAME {sel}"
    vp = Path(BIO); tmp = vp.with_name(vp.stem + "_orphanfill.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    dup = con.execute(f"SELECT COUNT(*) FROM (SELECT NFL_player_id FROM '{tq}' WHERE NFL_player_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1)").fetchone()[0]
    orphan_after = con.execute(f"""SELECT COUNT(*) FROM (SELECT DISTINCT NFL_player_id FROM '{wk}' WHERE NFL_player_id IS NOT NULL) s
        LEFT JOIN '{tq}' b USING(NFL_player_id) WHERE b.NFL_player_id IS NULL""").fetchone()[0]
    con.close()
    gate = (after == before + n_orphan) and (dup == 0) and (orphan_after == 0)
    res = {"before": before, "after": after, "added": after - before, "dup_id": dup,
           "orphan_after": orphan_after, "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        from .recon_common import utc_stamp
        bk = vp.with_name(vp.stem + f"_preorphanfill_{utc_stamp()}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
        res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    if not a.apply:
        print("DRY:", run())
    else:
        r = run(apply=True)
        print(f"bio rows {r['before']:,}->{r['after']:,} (+{r['added']}) | dup_id={r['dup_id']} orphan_after={r['orphan_after']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
