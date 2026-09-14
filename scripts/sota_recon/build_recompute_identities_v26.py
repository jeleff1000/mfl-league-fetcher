"""
sota_recon/build_recompute_identities_v26.py -- Cat A: recompute every in-row derived identity to its
formula in one streaming rewrite.

build_derived_columns_v26 only ADDS columns (it refuses to overwrite existing ones), so after a component
fill -- e.g. the fumbles_lost 1978-93 fill -- the derived cells go stale (turnovers 5,554 mismatches; the
rate stats/pat_missed drift too). This sets each derived col = its recon_row_identities formula WHERE the
guard holds, leaving guard-fail rows untouched. Formulas are imported from recon_row_identities so the
builder and the checker can never disagree.

These derived cells are DISPLAY derivations of components that already feed points (turnovers = INT +
fumbles_lost, etc.) -- none is itself a scoring input -- so golden fpts is invariant (gated to prove it).

    python -m scripts.sota_recon.build_recompute_identities_v26 [--apply]
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
from .recon_row_identities import IDENTITIES


def _mismatch(con, tq: str, col: str, formula: str, guard: str, tol: float) -> int:
    return con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{tq}') "
        f"WHERE {guard} AND {col} IS NOT NULL AND ABS({col} - ({formula})) > {tol}"
    ).fetchone()[0]


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_idrc_{utc_stamp()}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("SET memory_limit='1500MB'")   # box runs saturated (corpus batch); spill instead of OOM
    con.execute("PRAGMA threads=1")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{vq}')").fetchall()}

    targets = [(n, c, f, g, t) for (n, c, f, g, t) in IDENTITIES if c in cols]
    repls = [f"CASE WHEN ({g}) THEN ({f}) ELSE {c} END AS {c}" for (_, c, f, g, _) in targets]
    sql = f"SELECT * REPLACE ({', '.join(repls)}) FROM read_parquet('{vq}')"

    before = {n: _mismatch(con, vq, c, f, g, t) for (n, c, f, g, t) in targets}
    before_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{vq}')").fetchone()[0]
    if not apply:
        con.close()
        return {"targets": [t[0] for t in targets], "before_mismatch": before, "rows": before_rows}

    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute("SET max_temp_directory_size='40GB'")
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_recompute.parquet")
    rb = con.execute(sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), rb.schema)
    for batch in rb:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()

    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    after = {n: _mismatch(con, tq, c, f, g, t) for (n, c, f, g, t) in targets}
    con.close()

    # gate: golden fpts invariant + rows unchanged + every identity now exactly satisfied
    from . import golden_points, sources as S
    old = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_points.run()
    finally:
        S.latest_v26 = old
    gate = (after_rows == before_rows and g["passed_overall"] and all(v == 0 for v in after.values()))

    res = {"before_mismatch": before, "after_mismatch": after,
           "golden_pass": g["passed_overall"], "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_prerecompute_{stamp}.parquet")
        shutil.copy2(vp, backup)
        from .recon_common import safe_replace; safe_replace(tmp, vp)
        res["backup"] = backup.name
        res["swapped"] = True
    else:
        res["swapped"] = False
        res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(run(apply=a.apply))
