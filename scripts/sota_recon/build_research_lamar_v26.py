"""
sota_recon/build_research_lamar_v26.py  --  recompute all 56 research LAMAR columns on v26 (local table).

Uses the rewired research_lamar.py (56 configs: base 36 + 8 TEP + 12 PPFD, population
percentiles, percentile-of-pool replacement). Snapshots existing LAMAR columns, recomputes all
supported columns, DIFFS old-vs-new (for review), gates, writes.

MEMORY DISCIPLINE (wave57 lesson: this box can have <2GB free): the compute runs on a
NARROW projection (keys + position + the fpts source columns the configs reference) in
a file-backed DuckDB with a low memory cap; the wide table is never materialized --
LAMAR columns attach via a batch-aligned pyarrow zip with a player_week assert.

Gated: golden_samples pass, rows unchanged, all 56 lamar cols populated. Old/new diff printed.

    python -m scripts.sota_recon.build_research_lamar_v26 [--apply]
"""

from __future__ import annotations
import argparse
import os
import re
import shutil
import sys
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

_FFS = Path(__file__).resolve().parent.parent.parent / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))
from multi_league.data_fetchers.research_lamar import (
    get_all_lamar_columns,
    build_lamar_enrichment_sql_fast,
)

PROV = "wave57.research_lamar_56"
STAMP_POSITIONS = ["QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB"]


def run(apply=False):
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    cols = get_all_lamar_columns()
    if not apply:
        return len(cols)
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=true")  # _rid must be file order
    con.execute("SET memory_limit='1200MB'")
    con.execute(f"SET temp_directory='{sp}'")

    schema = pq.ParquetFile(v26).schema_arrow
    existing = set(schema.names)
    missing_out = [c for c in cols if c not in existing]
    if missing_out:
        con.close()
        shutil.rmtree(sp, ignore_errors=True)
        raise RuntimeError(f"lamar columns not in table schema: {missing_out[:8]}")

    updates = build_lamar_enrichment_sql_fast(table_name="nt")
    # every source column the generated SQL references, from the SQL itself
    sql_text = " ".join(s for _, stmts in updates for s in stmts)
    referenced = set(re.findall(r"\b(?:fpts|pts)_[a-z0-9_]+\b", sql_text))
    src_cols = sorted((referenced & existing) - set(cols))

    keys = ["player_week", "year", "week", "season_type", "NFL_player_id", "position"]
    con.execute(f"""CREATE TABLE nt AS
        SELECT {", ".join(f'"{c}"' for c in keys + src_cols)},
               {", ".join(f'"{c}" AS "_old_{c}"' for c in cols)}
        FROM '{vq}'""")
    con.execute("ALTER TABLE nt ADD COLUMN _rid BIGINT")
    con.execute("UPDATE nt SET _rid=rowid")
    before = con.execute("SELECT COUNT(*) FROM nt").fetchone()[0]
    for c in cols:
        con.execute(f'ALTER TABLE nt ADD COLUMN "{c}" DOUBLE')

    for col, stmts in updates:
        for s in stmts:
            con.execute(s)

    popn = {c: con.execute(f'SELECT COUNT(*) FROM nt WHERE "{c}" IS NOT NULL').fetchone()[0]
            for c in cols}
    allpop = all(v > 0 for v in popn.values())
    diff = []
    for c in cols:
        r = con.execute(f"""SELECT COUNT(*) tot,
              SUM(CASE WHEN abs(COALESCE("_old_{c}",0)-"{c}")>0.05 THEN 1 ELSE 0 END) chg,
              ROUND(AVG(abs(COALESCE("_old_{c}",0)-COALESCE("{c}",0))),3) mad
            FROM nt WHERE "_old_{c}" IS NOT NULL OR "{c}" IS NOT NULL""").fetchone()
        diff.append((c, r[0], r[1] or 0, r[2] or 0))

    lam_pq = os.path.join(sp, "lamar.parquet")
    con.execute(f"""COPY (SELECT player_week, {", ".join(f'"{c}"' for c in cols)}
                    FROM nt ORDER BY _rid) TO '{Path(lam_pq).as_posix()}'""")
    con.close()

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_lam56.parquet")
    pf_main = pq.ParquetFile(v26)
    pf_lam = pq.ParquetFile(lam_pq)
    writer = None
    after = 0
    try:
        for mb, lb in zip(pf_main.iter_batches(batch_size=50000),
                          pf_lam.iter_batches(batch_size=50000)):
            names = mb.schema.names
            pw_i = names.index("player_week")
            log_i = names.index("recon_correction_log")
            pos_i = names.index("position")
            if mb.column(pw_i).to_pylist() != lb.column(0).to_pylist():
                raise RuntimeError("batch alignment lost between source and lamar")
            arrays = list(mb.columns)
            for c in cols:
                ci = names.index(c)
                arrays[ci] = lb.column(lb.schema.names.index(c)).cast(
                    mb.schema.field(ci).type)
            pos = mb.column(pos_i).to_pylist()
            log = mb.column(log_i).to_pylist()
            new_log = []
            for p, lg in zip(pos, log):
                eligible = p is not None and any(
                    t in STAMP_POSITIONS for t in str(p).split(","))
                if eligible and (not lg or PROV not in lg):
                    new_log.append((lg + "," if lg else "") + PROV)
                else:
                    new_log.append(lg)
            arrays[log_i] = pa.array(new_log, type=mb.schema.field(log_i).type)
            out = pa.record_batch(arrays, schema=mb.schema)
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), mb.schema)
            writer.write_batch(out)
            after += out.num_rows
    finally:
        if writer is not None:
            writer.close()
        pf_main.close()
        pf_lam.close()
    shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S

    o = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and allpop
    res = {
        "before": before,
        "after": after,
        "n_cols": len(cols),
        "all_populated": allpop,
        "golden": f"{g['passed']}/{g['total']}",
        "gate_pass": bool(gate),
        "diff": diff,
        "temp": str(tmp),
    }
    if gate:
        bk = vp.with_name(vp.stem + f"_prelam56_{stamp}.parquet")
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
    if not a.apply:
        print(f"would compute {run()} research LAMAR columns")
    else:
        r = run(apply=True)
        print(
            f"rows {r['before']:,}->{r['after']:,} | {r['n_cols']} lamar cols all_populated={r['all_populated']} | golden {r['golden']}"
        )
        print(
            f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
            + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}")
        )
        print("== old-vs-new diff (existing configs): col | rows | changed(>0.05) | mean-abs-delta ==")
        for c, tot, chg, mad in sorted(r["diff"], key=lambda x: -x[2])[:40]:
            print(f"  {c:<28} {tot:>8,} {chg:>8,} {mad}")
