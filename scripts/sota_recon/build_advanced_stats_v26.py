"""
sota_recon/build_advanced_stats_v26.py  --  fold the validated advanced PBP atoms +
composites onto the v26 super table IN PLACE, the same way every other build_*_v26 step
evolves the release (stream via record batches, gate, backup + atomic swap).

Adds 18 atoms (success x6, WPA x3, explosive x3, red-zone x6) + 5 composites
(total_epa/total_wpa/scrimmage_yards/total_tds_accounted_for/total_touches), joined by
(NFL_player_id, year, week) from the PBP weekly rollup. EPA/WP-model atoms (WPA, success)
are NULL before 1999 (the win-prob era); explosive/red-zone are model-free and extend to
1978. EPA/air_yards/cpoe/2pt already live in the super table and are untouched. See
docs/advanced-stats-registry.json for the per-stat formula + baseline.

Gated: row count unchanged, all 5 composites exactly equal their component sums, all 23
columns present, and golden_samples still passes (additive columns must not perturb any
known-truth anchor) -> backup + swap. Idempotent: re-running re-derives the columns.

    python -m scripts.sota_recon.build_advanced_stats_v26            # dry-run
    python -m scripts.sota_recon.build_advanced_stats_v26 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts.integrate_advanced_atoms_into_supertable import (  # noqa: E402
    COMPOSITES,
    DEFAULT_ROLLUP,
    NEW_ATOMS,
    rollup_agg_sql,
    select_sql,
)
from .recon_common import utc_stamp  # noqa: E402
from .sources import latest_v26  # noqa: E402

ADDED = NEW_ATOMS + list(COMPOSITES.keys())
COMPONENTS = {name: [p.split(".", 1)[1] for p in parts] for name, parts in COMPOSITES.items()}


def _composite_gate(con: duckdb.DuckDBPyConnection, tq: str) -> int:
    bad = 0
    for name, comps in COMPONENTS.items():
        all_null = " AND ".join(f"{c} IS NULL" for c in comps)
        summed = " + ".join(f"COALESCE({c},0)" for c in comps)
        bad += con.execute(
            f"SELECT COUNT(*) FROM {tq} WHERE {name} IS NOT NULL AND ABS({name}-({summed}))>1e-6"
        ).fetchone()[0]
        bad += con.execute(
            f"SELECT COUNT(*) FROM {tq} WHERE {name} IS NULL AND NOT ({all_null})"
        ).fetchone()[0]
    return int(bad)


def run(apply: bool, rollup: Path = DEFAULT_ROLLUP) -> dict:
    v26 = latest_v26()
    stamp = utc_stamp()
    if not rollup.exists():
        raise FileNotFoundError(f"rollup not found: {rollup}")

    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='1500MB'")
    _tmp = Path("D:/league-history-data/nfl/tmp/duckdb")
    _tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{_tmp.as_posix()}'")

    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{Path(v26).as_posix()}')").fetchall()]
    existing_new = [c for c in ADDED if c in cols]
    con.execute(f"CREATE TEMP TABLE rollup_agg AS {rollup_agg_sql(rollup)}")

    before = con.execute(f"SELECT COUNT(*) FROM read_parquet('{Path(v26).as_posix()}')").fetchone()[0]
    info = {"existing_new_cols": len(existing_new), "adds": len(ADDED)}
    if not apply:
        con.close()
        return {"before": int(before), "info": info, "swapped": False}

    sql = select_sql(Path(v26).as_posix(), exclude_cols=existing_new or None)
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_advstats.parquet")
    rdr = con.execute(sql).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema, compression="zstd")
    for b in rdr:
        w.write_batch(b)
    w.close()

    tq = f"read_parquet('{tmp.as_posix()}')"
    after = con.execute(f"SELECT COUNT(*) FROM {tq}").fetchone()[0]
    out_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {tq}").fetchall()}
    missing = [c for c in ADDED if c not in out_cols]
    comp_bad = _composite_gate(con, tq)
    con.close()

    # golden anchors: additive columns must not move any known-truth value.
    import scripts.sota_recon.sources as S
    o = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        from . import golden_samples
        g = golden_samples.run()
    finally:
        S.latest_v26 = o

    gate = (after == before and not missing and comp_bad == 0 and g["failed"] == 0)
    res = {"before": int(before), "after": int(after), "info": info, "missing_cols": missing,
           "composite_violations": comp_bad, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preadvstats_backup_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(a.apply)
    if not a.apply:
        print("DRY-RUN:", r["info"], "| rows", f"{r['before']:,}")
    else:
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']} | "
              f"missing {r['missing_cols']} | composite_violations {r['composite_violations']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
