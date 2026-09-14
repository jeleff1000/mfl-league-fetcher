"""
sota_recon/build_primary_position_v26.py  --  denormalize a stable per-player primary_position.

Career-rank population needs one position per player, but the weekly `position` varies across a
career for ~1,588 players (two-way guys, position switches), and player_bio.nfl_position -- the
authoritative primary position -- is only ~96% populated. So this stamps a single
`primary_position` = COALESCE(bio.nfl_position, dominant-weekly-position) on every row, computed
ONCE here instead of a MODE aggregate in every weekly rank rebuild. Career ranks then read the
column directly (a player attribute, not a per-rebuild computation); season ranks keep using the
per-year `position` (which is constant within a season).

Gated: primary_position non-null for every row that has a weekly position, row count unchanged,
golden holds -> backup + swap.

    python -m scripts.sota_recon.build_primary_position_v26          # dry-run (coverage)
    python -m scripts.sota_recon.build_primary_position_v26 --apply  # gated
"""
from __future__ import annotations
import argparse
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave51.primary_position"
PROV_COL = "recon_correction_log"


def _build_pp(con, src: str) -> None:
    """primary_position per player = bio.nfl_position, else dominant weekly position."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _pp AS
        WITH dom AS (
            SELECT NFL_player_id, MODE(position) dompos FROM {src}
            WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL AND position <> '' GROUP BY 1
        ),
        bio AS (SELECT NFL_player_id, NULLIF(nfl_position, '') bpos FROM read_parquet('{Path(BIO).as_posix()}'))
        SELECT d.NFL_player_id, COALESCE(b.bpos, d.dompos) AS primary_position
        FROM dom d LEFT JOIN bio b USING (NFL_player_id)""")


def run(apply: bool) -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    con = duckdb.connect()
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='8GB'")
    src = f"read_parquet('{Path(v26).as_posix()}')"
    before = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    already = "primary_position" in [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    _build_pp(con, src)
    cov = con.execute(f"""SELECT COUNT(*), SUM(CASE WHEN primary_position IS NULL THEN 1 ELSE 0 END) FROM _pp""").fetchone()
    pre = {"players": int(cov[0]), "null_primary": int(cov[1]), "column_exists": already}

    if not apply:
        con.close()
        return {"before": int(before), "pre": pre, "swapped": False}

    # Stream the wide table + the joined primary_position column (+ provenance).
    log = (f"CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
           f"WHEN {PROV_COL} LIKE '%{PROV}%' THEN {PROV_COL} ELSE {PROV_COL}||',{PROV}' END")
    exclude = "EXCLUDE (primary_position)" if already else ""
    transform = f"""
        SELECT s.* {exclude} REPLACE ({log} AS {PROV_COL}), p.primary_position AS primary_position
        FROM {src} s LEFT JOIN _pp p USING (NFL_player_id)
    """
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_pptmp.parquet")
    rdr = con.execute(transform).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema)
    for b in rdr:
        w.write_batch(b)
    w.close()
    tq = f"read_parquet('{tmp.as_posix()}')"
    after = con.execute(f"SELECT COUNT(*) FROM {tq}").fetchone()[0]
    # gate: every row with a weekly position now has a primary_position
    null_pp = con.execute(f"""SELECT COUNT(*) FROM {tq}
        WHERE position IS NOT NULL AND position <> '' AND (primary_position IS NULL OR primary_position='')""").fetchone()[0]
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0 and after == before and null_pp == 0)
    res = {"before": int(before), "after": int(after), "pre": pre, "null_primary_rows": int(null_pp),
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        import shutil, os
        bk = vp.with_name(vp.stem + f"_preprimarypos_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(a.apply)
    if not a.apply:
        print("DRY-RUN:", r["pre"])
    else:
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']} | null_primary_rows {r['null_primary_rows']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
