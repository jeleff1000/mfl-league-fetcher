"""
sota_recon/build_weekly_ranks_v26.py  --  rerank ALL formats weekly on v26 (local weekly super table).

Data changed (atom reconciliation + new fpts_ppfd/tep), so rerank every weekly position/flex/sflex
format consistently with current fpts. Weekly specs are derived from the canonical rank_specs_for_scope
('season' prefix stripped to weekly), so they match the season/alltime ranks exactly and now include
rank_*_ppfd + rank_sflex_*_tep/ppfd. Weekly rank = ROW_NUMBER per (year,week,season_type) over the
position pool, ORDER BY points DESC, ties by NFL_player_id; only players with a points value (IS NOT NULL).

MEMORY DISCIPLINE (wave57 lesson: this box can have <2GB free): the compute runs on a
NARROW projection (keys + points cols) in a file-backed DuckDB with a low memory cap,
and the wide table is never materialized -- rank columns attach via a batch-aligned
pyarrow zip of the source parquet and the computed ranks parquet, with a per-batch
player_week alignment assert.

Gated: golden_samples pass, rows unchanged, all weekly rank cols populated.

    python -m scripts.sota_recon.build_weekly_ranks_v26 [--apply]
"""

from __future__ import annotations
import argparse
import os
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
from multi_league.data_fetchers.aggregate_nfl_stats_fly import rank_specs_for_scope

PROV = "wave57.weekly_ranks_allfmt"


def _weekly_specs():
    # (weekly_col, points_col, positions) from canonical season specs, prefix-stripped to weekly
    out = []
    for s in rank_specs_for_scope("season"):
        col = s.col.replace("rank_season_", "rank_")
        out.append((col, s.points_col, tuple(s.positions)))
    return out


def run(apply=False):
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    specs = _weekly_specs()
    if not apply:
        return len(specs)
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
    poscol = "position"
    computed, skipped = [], []
    for col, pts, positions in specs:
        if pts not in existing:
            skipped.append((col, pts, "no-col"))
            continue
        has = con.execute(
            f"SELECT COUNT(*) FROM '{vq}' WHERE TRY_CAST({pts} AS DOUBLE) IS NOT NULL "
            f"AND TRY_CAST({pts} AS DOUBLE)<>0").fetchone()[0]
        if has == 0:
            skipped.append((col, pts, "empty-src"))
            continue
        computed.append((col, pts, positions))
    missing_out = [c for c, _, _ in computed if c not in existing]
    if missing_out:
        con.close()
        shutil.rmtree(sp, ignore_errors=True)
        raise RuntimeError(f"output columns not in table schema: {missing_out[:8]}")

    narrow = ["player_week", "year", "week", "season_type", "NFL_player_id", poscol]
    narrow += sorted({pts for _, pts, _ in computed if pts not in narrow})
    con.execute(f"""CREATE TABLE nt AS
        SELECT {", ".join(f'"{c}"' for c in narrow)} FROM '{vq}'""")
    con.execute("ALTER TABLE nt ADD COLUMN _rid BIGINT")
    con.execute("UPDATE nt SET _rid=rowid")
    before = con.execute("SELECT COUNT(*) FROM nt").fetchone()[0]

    for col, pts, positions in computed:
        plist = "[" + ",".join(f"'{p}'" for p in positions) + "]"
        con.execute(f'ALTER TABLE nt ADD COLUMN "{col}" INTEGER')
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _stg AS
            SELECT _rid, ROW_NUMBER() OVER (PARTITION BY year,week,season_type
                   ORDER BY TRY_CAST({pts} AS DOUBLE) DESC, NFL_player_id) AS r
            FROM nt WHERE list_has_any(string_split(COALESCE({poscol}, ''), ','), {plist})
              AND {pts} IS NOT NULL""")
        con.execute(f'UPDATE nt SET "{col}"=_stg.r FROM _stg WHERE nt._rid=_stg._rid')

    popn = {col: con.execute(f'SELECT COUNT(*) FROM nt WHERE "{col}" IS NOT NULL').fetchone()[0]
            for col, _, _ in computed}
    allpop = all(v > 0 for v in popn.values())
    rank_cols = [col for col, _, _ in computed]
    ranks_pq = os.path.join(sp, "ranks.parquet")
    con.execute(f"""COPY (SELECT player_week, {", ".join(f'"{c}"' for c in rank_cols)}
                    FROM nt ORDER BY _rid) TO '{Path(ranks_pq).as_posix()}'""")
    con.close()

    # attach: batch-aligned zip of source parquet and ranks parquet (same row order)
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_wrank.parquet")
    pf_main = pq.ParquetFile(v26)
    pf_rank = pq.ParquetFile(ranks_pq)
    writer = None
    after = 0
    try:
        for mb, rb in zip(pf_main.iter_batches(batch_size=50000),
                          pf_rank.iter_batches(batch_size=50000)):
            names = mb.schema.names
            pw_i = names.index("player_week")
            log_i = names.index("recon_correction_log")
            pos_i = names.index(poscol)
            if mb.column(pw_i).to_pylist() != rb.column(0).to_pylist():
                raise RuntimeError("batch alignment lost between source and ranks")
            arrays = list(mb.columns)
            for c in rank_cols:
                ci = names.index(c)
                arrays[ci] = rb.column(rb.schema.names.index(c)).cast(
                    mb.schema.field(ci).type)
            pos = mb.column(pos_i).to_pylist()
            log = mb.column(log_i).to_pylist()
            new_log = []
            for p, lg in zip(pos, log):
                if p is not None and (not lg or PROV not in lg):
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
        pf_rank.close()
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
        "n_computed": len(computed),
        "n_skipped": len(skipped),
        "skipped": skipped,
        "all_populated": allpop,
        "new_cols": [],
        "golden": f"{g['passed']}/{g['total']}",
        "gate_pass": bool(gate),
        "temp": str(tmp),
    }
    if gate:
        bk = vp.with_name(vp.stem + f"_prewrank_{stamp}.parquet")
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
        print(f"would rerank {run()} weekly rank formats")
    else:
        r = run(apply=True)
        print(
            f"rows {r['before']:,}->{r['after']:,} | computed {r['n_computed']} weekly rank formats (skipped {r['n_skipped']}) all_populated={r['all_populated']} | golden {r['golden']} | gate_pass={r['gate_pass']} swapped={r['swapped']}"
        )
