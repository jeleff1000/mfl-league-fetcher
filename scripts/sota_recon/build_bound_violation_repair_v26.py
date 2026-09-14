"""
sota_recon/build_bound_violation_repair_v26.py  --  wave55: the 11 hard bound violations
(passing_interceptions > attempts), each adjudicated by witness (2026-07-12, Joe's
Masterson report -- the founding defect of the whole closeout).

Three adjudicated classes (evidence per row, never a blanket rule):

  MASTERSON (MastBe20_1935_12 vs CRD): box line 0 INT / 4 att / 2 cmp / 49 yds -- his
  att/cmp/yds match v26 EXACTLY, only the INT cell is corrupt. Season conservation
  proves it to the digit: pages 1935 = 44/18/446/4; his v26 weeks sum 44/18/446/13;
  removing the bogus 9 -> 4 exact.  passing_interceptions 9 -> 0.

  PHANTOM (Keyshawn Johnson 00-0008561_2006_10): pbp rollup (licensed 100.0% for
  passing_interceptions) shows 0/0/0/0 -- no passing involvement at all.  1 -> 0.

  FALSE-ZERO ATTEMPTS (9 rows, 1933-1947): the box CONFIRMS each INT count but the
  era's attempt charting is blank -- an interception proves >= 1 attempt, so the
  stored attempts (0, or 1 under a 4-INT game) are false zeros, not measurements.
  attempts -> NULL (unknown, never fabricated; the bound resolves because NULL rows
  are exempt). No sum changes (0 -> NULL).

Every fix asserts the CURRENT value (stale-guard) and emits a cell_override fact.
Gates: only listed cells change; row count unchanged; bound violations 11 -> 0.

    python -m scripts.sota_recon.build_bound_violation_repair_v26            # DRY RUN
    python -m scripts.sota_recon.build_bound_violation_repair_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import latest_v26

WAVE = "wave55.bound_violation_repair"
BATCH = 131_072

# player_week -> (column, asserted_old, new, witness)
REPAIRS: dict[str, tuple[str, float | None, float | None, str]] = {
    "MastBe20_1935_12_G193512010chi_CHI_5": (
        "passing_interceptions", 9.0, 0.0,
        "box 193512010chi: 0 INT/4 att/2 cmp/49 yds (att/cmp/yds match v26 exactly); "
        "pages 1935 season 4 INT = v26 sum after fix (44/18/446 already exact)"),
    "00-0008561_2006_10": (
        "passing_interceptions", 1.0, 0.0,
        "pbp rollup (licensed 100.0%): Keyshawn Johnson 2006 wk10 = 0 att/0 int; "
        "phantom cell"),
    # false-zero attempts: box confirms the INTs, era attempts unrecorded -> NULL
    "PresGl20_1933_3": ("attempts", 0.0, None, "box int=2 confirmed; att>=int; era att unrecorded"),
    "PresGl20_1933_8": ("attempts", 0.0, None, "box int=1 confirmed; att>=int; era att unrecorded"),
    "PresGl20_1933_9": ("attempts", 0.0, None, "box int=1 confirmed; att>=int; era att unrecorded"),
    "PresGl20_1933_10": ("attempts", 0.0, None, "box int=2 confirmed; att>=int; era att unrecorded"),
    "PresGl20_1933_12": ("attempts", 1.0, None, "box int=4 confirmed; att>=int; era att unrecorded"),
    "HinkCl20_1934_5": ("attempts", 0.0, None, "box int=1 confirmed; att>=int; era att unrecorded"),
    "HIST-99594971_1941_6": ("attempts", 0.0, None, "box int=1 confirmed; att>=int; era att unrecorded"),
    "HoppHa20_1941_12": ("attempts", 0.0, None, "box int=1 confirmed; att>=int; era att unrecorded"),
    "MargJo20_1947_2": ("attempts", 0.0, None, "box int=1 confirmed; att>=int; era att unrecorded"),
}


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    v26_p = Path(latest_v26())
    vq = Path(v26_p).as_posix()

    pws = ", ".join(f"'{k}'" for k in REPAIRS)
    cur = {pw: (i, a) for pw, i, a in con.execute(f"""
        SELECT player_week, passing_interceptions, attempts
        FROM '{vq}' WHERE player_week IN ({pws})""").fetchall()}
    stale, ok = [], {}
    for pw, (col, old, new, wit) in REPAIRS.items():
        row = cur.get(pw)
        if row is None:
            stale.append((pw, "ROW MISSING"))
            continue
        val = row[0] if col == "passing_interceptions" else row[1]
        if val != old:
            stale.append((pw, f"{col} is {val}, asserted {old}"))
        else:
            ok[pw] = (col, old, new, wit)
    before_viol = con.execute(f"""
        SELECT COUNT(*) FROM '{vq}'
        WHERE passing_interceptions > attempts
          AND passing_interceptions IS NOT NULL AND attempts IS NOT NULL""").fetchone()[0]
    diag = {"repairs": len(REPAIRS), "appliable": len(ok), "stale": stale,
            "bound_violations_before": before_viol}
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}
    if stale:
        con.close()
        return {"mode": "APPLY-REFUSED", **diag}

    before_n = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    tmp = v26_p.with_name(v26_p.stem + "_w55.parquet")
    pf = pq.ParquetFile(vq)
    writer = None
    try:
        for batch in pf.iter_batches(batch_size=BATCH):
            tbl = pa.Table.from_batches([batch])
            pw_list = tbl.column("player_week").to_pylist()
            hits = [i for i, p in enumerate(pw_list) if p in ok]
            if hits:
                ints = tbl.column("passing_interceptions").to_pylist()
                atts = tbl.column("attempts").to_pylist()
                for i in hits:
                    col, _, new, _ = ok[pw_list[i]]
                    if col == "passing_interceptions":
                        ints[i] = new
                    else:
                        atts[i] = new
                tbl = tbl.set_column(tbl.schema.get_field_index("passing_interceptions"),
                                     "passing_interceptions", pa.array(ints, pa.float64()))
                tbl = tbl.set_column(tbl.schema.get_field_index("attempts"),
                                     "attempts", pa.array(atts, pa.float64()))
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), tbl.schema)
            writer.write_table(tbl)
    finally:
        if writer is not None:
            writer.close()
        pf.close()

    tq = Path(tmp).as_posix()
    after_n = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_viol = con.execute(f"""
        SELECT COUNT(*) FROM '{tq}'
        WHERE passing_interceptions > attempts
          AND passing_interceptions IS NOT NULL AND attempts IS NOT NULL""").fetchone()[0]
    mast = con.execute(f"""
        SELECT passing_interceptions FROM '{tq}'
        WHERE player_week = 'MastBe20_1935_12_G193512010chi_CHI_5'""").fetchone()[0]
    gate = after_n == before_n and after_viol == 0 and mast == 0.0
    res = {"mode": "APPLY", **diag, "rows": [before_n, after_n],
           "bound_violations_after": after_viol, "masterson_int_after": mast,
           "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        for pw, (col, old, new, wit) in ok.items():
            # NULL-ing corrections encode new_value as the literal 'NULL' (the fact
            # schema requires a non-None new_value; replay treats 'NULL' as SQL NULL)
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw, column_name=col,
                            old_value=str(old),
                            new_value="NULL" if new is None else str(new),
                            wave_id=WAVE,
                            reason="hard bound passing_interceptions<=attempts; "
                                   "witnessed adjudication (see witness)",
                            witness=wit, source_snapshot_id=snap)
        fc.close()
        bk = v26_p.with_name(v26_p.stem + f"_prew55_{utc_stamp()}.parquet")
        shutil.copy2(v26_p, bk)
        os.replace(tmp, v26_p)
        res.update(backup=str(bk), swapped=True)
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
