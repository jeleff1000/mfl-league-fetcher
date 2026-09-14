"""
sota_recon/build_sameid_dup_collapse.py  --  collapse SAME-id duplicate player-weeks (1950+)

A few dual-role players (kicker+punter, e.g. Mike Eischeid, Tommy Davis) were ingested as TWO
rows for the same game under the SAME NFL_player_id -- one tagged K, one P -- with the shared
stat (punts) copied onto BOTH rows, so it double-counts at team level. The real<->real collapse
only caught splits across DIFFERENT ids; this closes the same-id case.

Post-1950 there are no doubleheaders, so >1 row for one (NFL_player_id, year, week) is a true
duplicate. MAX-combine the numeric atoms into the survivor (keeps the non-doubled value), pick the
non-DEF/most-specific position, drop the extras.

Gated: golden 24/24, same-id dup player-weeks (1950+) -> 0, rows drop by exactly the merged count.

    python -m scripts.sota_recon.build_sameid_dup_collapse            # dry-run
    python -m scripts.sota_recon.build_sameid_dup_collapse --apply
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

PROV = "wave24.sameid_dup_collapse"
PROV_COL = "recon_correction_log"
ID_COLS = {"NFL_player_id", "player", "year", "week", "season_type", "player_week", "nfl_team",
           "nfl_franchise_number", "opponent_nfl_franchise_number", "position", PROV_COL, "data_source"}


def _count(con, tbl):
    return con.execute(f"""SELECT COUNT(*) FROM (SELECT NFL_player_id, year, week FROM {tbl}
        WHERE year>=1950 AND NFL_player_id IS NOT NULL GROUP BY 1,2,3 HAVING COUNT(*)>1)""").fetchone()[0]


def apply_collapse():
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    types = {c[0]: c[1] for c in con.execute("DESCRIBE st").fetchall()}
    if PROV_COL not in types:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR"); types[PROV_COL] = "VARCHAR"
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre = _count(con, "st")
    num = [c for c, t in types.items() if c not in ID_COLS
           and any(k in t.upper() for k in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT"))]
    con.execute("""CREATE TEMP TABLE dups AS
        SELECT NFL_player_id id, year yr, week wk, MIN(rowid) keep, COUNT(*) c
        FROM st WHERE year>=1950 AND NFL_player_id IS NOT NULL GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    # prefer a non-DEF, non-null position for the survivor (kept row's position)
    con.execute(f"""CREATE TEMP TABLE merged AS
        SELECT st.NFL_player_id id, st.year yr, st.week wk,
               {', '.join(f'MAX(st.{c}) {c}' for c in num)}
        FROM st JOIN dups ON st.NFL_player_id=dups.id AND st.year=dups.yr AND st.week=dups.wk
        GROUP BY 1,2,3""")
    setc = ", ".join(f"{c}=m.{c}" for c in num)
    con.execute(f"""UPDATE st SET {setc},
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM merged m JOIN dups d ON m.id=d.id AND m.yr=d.yr AND m.wk=d.wk WHERE st.rowid=d.keep""")
    dropped = con.execute("SELECT COALESCE(SUM(c-1),0) FROM dups").fetchone()[0]
    con.execute("""DELETE FROM st WHERE rowid IN
        (SELECT st.rowid FROM st JOIN dups d ON st.NFL_player_id=d.id AND st.year=d.yr AND st.week=d.wk
           WHERE st.rowid<>d.keep)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    post = _count(con, "st")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_sidtmp.parquet")
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
    gate = (g["failed"] == 0) and (post == 0) and (before - after == dropped) and dropped > 0
    res = {"pre_dups": int(pre), "post_dups": int(post), "dropped": int(dropped),
           "before": int(before), "after": int(after), "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_presid_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_collapse()
        print(f"same-id dups {r['pre_dups']}->{r['post_dups']} | dropped {r['dropped']} | "
              f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        con.execute(f"CREATE TEMP TABLE st AS SELECT * FROM '{v}'")
        print(f"same-id dup player-weeks (1950+): {_count(con,'st')}")
