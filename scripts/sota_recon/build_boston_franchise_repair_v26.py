"""
sota_recon/build_boston_franchise_repair_v26.py  --  untangle the 1940s Boston franchises.

The PFR schedule authority (nfl_team_games_all) merges two defunct Boston franchises into
the Redskins/Commanders (franchise 4): the 1929 Boston Bulldogs and the 1944-48 Boston Yanks
all resolve to fid 4. So the super table inherited a leak that contaminates the modern
Redskins record AND misroutes individual players (Joe Aguirre's 1944 Boston Yanks season sits
in the Redskins' all-time roster). This corrects it to the NFL-approved lineage:

  * 1929 Boston Bulldogs  = the Pottsville Maroons (fr 112) relocated to Boston for one
    season, then folded  ->  move all BOS/1929 rows to franchise 112.
  * 1944-48 Boston Yanks  = a distinct defunct franchise (fr 148)  ->  move the 154 BOS/1944
    rows still stuck in franchise 4 to 148, joining the 1945-48 Yanks already there.
  * Redskins/Commanders (fr 4) then = BOS 1932-36 + WAS 1937+, clean and distinct.

Fixes players AND DST (the leak reached both). DST ids are re-hardened to DEF-{fn} by the
follow-on build_franchise_normalization pass. Gated: Redskins hold no BOS outside 1932-36,
row count unchanged, no franchise loses/gains a stray code, golden holds -> backup + swap.

    python -m scripts.sota_recon.build_boston_franchise_repair_v26          # dry-run
    python -m scripts.sota_recon.build_boston_franchise_repair_v26 --apply  # gated
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

PROV = "wave50.boston_franchise_repair"
PROV_COL = "recon_correction_log"

# franchise -> canonical DST display name (franchise-level; per-year code stays era-accurate)
DST_NAME = {112: "Pottsville Maroons DST", 148: "Boston Yanks DST"}


# New franchise number for a row (BOS/1929 -> 112 Pottsville; BOS/1944-48 in the Redskins
# -> 148 Boston Yanks; everything else unchanged). Used for both the id and the DST rename.
_NEW_FN = """CASE
    WHEN nfl_team='BOS' AND CAST(year AS INT)=1929 THEN 112
    WHEN nfl_team='BOS' AND CAST(year AS INT) BETWEEN 1944 AND 1948 AND nfl_franchise_number=4 THEN 148
    ELSE nfl_franchise_number END"""


def _audit(con, ref: str) -> dict:
    return {
        "redskins_bos_out_of_era": con.execute(f"""SELECT COUNT(*) FROM {ref}
            WHERE nfl_franchise_number=4 AND nfl_team='BOS' AND CAST(year AS INT) NOT BETWEEN 1932 AND 1936""").fetchone()[0],
        "bos_1929_not_112": con.execute(f"""SELECT COUNT(*) FROM {ref}
            WHERE nfl_team='BOS' AND CAST(year AS INT)=1929 AND nfl_franchise_number<>112""").fetchone()[0],
        "bos_yanks_in_redskins": con.execute(f"""SELECT COUNT(*) FROM {ref}
            WHERE nfl_team='BOS' AND CAST(year AS INT) BETWEEN 1944 AND 1948 AND nfl_franchise_number=4""").fetchone()[0],
    }


def _transform_sql(src: str) -> str:
    """Streaming SELECT * REPLACE that applies the reassignment + DST renames + provenance."""
    name_case = " ".join(
        f"WHEN position='DEF' AND ({_NEW_FN})={fn} THEN '{nm}'" for fn, nm in DST_NAME.items()
    )
    changed = f"({_NEW_FN}) <> nfl_franchise_number"
    return f"""
    SELECT * REPLACE (
        ({_NEW_FN}) AS nfl_franchise_number,
        CASE {name_case} ELSE player END AS player,
        CASE WHEN {changed} THEN
               CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}'
                    WHEN {PROV_COL} LIKE '%{PROV}%' THEN {PROV_COL}
                    ELSE {PROV_COL}||',{PROV}' END
             ELSE {PROV_COL} END AS {PROV_COL}
    )
    FROM {src}
    """


def run(apply: bool) -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    con = duckdb.connect()
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='8GB'")
    src = f"read_parquet('{Path(v26).as_posix()}')"
    before = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    pre = _audit(con, src)

    if not apply:
        con.close()
        return {"before": int(before), "pre": pre, "swapped": False}

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_bostmp.parquet")
    rdr = con.execute(_transform_sql(src)).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema)
    for b in rdr:
        w.write_batch(b)
    w.close()
    tq = f"read_parquet('{tmp.as_posix()}')"
    after = con.execute(f"SELECT COUNT(*) FROM {tq}").fetchone()[0]
    post = _audit(con, tq)
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0 and after == before
            and post["redskins_bos_out_of_era"] == 0
            and post["bos_1929_not_112"] == 0 and post["bos_yanks_in_redskins"] == 0)
    res = {"before": int(before), "after": int(after), "pre": pre, "post": post,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preboston_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(a.apply)
    if not a.apply:
        print("DRY-RUN pre-state:", r["pre"])
        print("  (would move 1929 BOS -> fr112, 1944-48 BOS in fr4 -> fr148, rename DSTs)")
    else:
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        print(f"  before: {r['pre']}")
        print(f"  after:  {r['post']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
