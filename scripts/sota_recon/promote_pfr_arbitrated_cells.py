"""
sota_recon/promote_pfr_arbitrated_cells.py -- wave60: correct v26 scoring cells where an
INDEPENDENT THIRD PARTY (PFR's boxscore scoring table) and the newspaper agree against v26.

This is the ONE sanctioned override path. The wave56/58 doctrine stands -- newspaper
evidence ALONE never overwrites a non-null v26 cell. Here the basis is different: PFR's
per-player scoring attribution (description_link_ids) is a third party, and where PFR's
count equals the newspaper's and both differ from v26, v26 is the outlier. Two independent
witnesses beat one thin ancient feed.

Self-contained: re-derives the arbitration at apply time from the live subject + PFR
scoring table (never trusts a stale CSV). Each override is a cell_override fact asserting
the exact old value; a per-cell STALE guard aborts if the live value isn't what was
adjudicated. Scorer atoms only (rushing_tds/receiving_tds/def_tds/fg_made/special_teams_tds)
-- the atoms PFR's scoring table can independently witness.

    python -m scripts.sota_recon.promote_pfr_arbitrated_cells            # DRY RUN
    python -m scripts.sota_recon.promote_pfr_arbitrated_cells --apply
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from . import facts
from .newspaper_witness_common import pfr_scoring_atom_case
from .sources import DATA_LAKE, latest_v26

WAVE = "wave60.pfr_arbitrated_override"
OUT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "newspaper_sidecar"
DIFFS = OUT / "g7_witnessed_diffs.csv"
SCORING = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables",
                       "scoring", "_combined.parquet").replace("\\", "/")
ATOMS = ("rushing_tds", "receiving_tds", "def_tds", "fg_made", "special_teams_tds")
BATCH = 131_072


def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _arbitrate(con) -> list[dict]:
    """Cells where PFR count == newspaper value != current v26 value (PFR backs paper)."""
    atom_case = pfr_scoring_atom_case("description")
    con.execute(f"""CREATE TEMP TABLE pfr AS
        WITH s AS (SELECT boxscore_id, description,
                          split_part(description_link_ids, ';', 1) pid
                   FROM read_parquet('{SCORING}')
                   WHERE boxscore_id < '194' AND description_link_ids IS NOT NULL
                     AND description_link_ids <> '')
        SELECT boxscore_id, pid, {atom_case} atom, COUNT(*) n
        FROM s GROUP BY 1, 2, {atom_case}""")
    rows = con.execute(f"""
        SELECT d.player_week, d.NFL_player_id, d.boxscore_id, d.atom,
               TRY_CAST(d.np_value AS DOUBLE) np, TRY_CAST(d.v26_value AS DOUBLE) v26, p.n
        FROM read_csv_auto('{DIFFS.as_posix()}') d
        JOIN pfr p ON p.boxscore_id = d.boxscore_id AND p.pid = d.NFL_player_id AND p.atom = d.atom
        WHERE d.atom IN {ATOMS} AND p.n = TRY_CAST(d.np_value AS DOUBLE)
          AND p.n <> TRY_CAST(d.v26_value AS DOUBLE)
        ORDER BY d.player_week, d.atom""").fetchall()
    return [{"player_week": r[0], "pid": r[1], "boxscore_id": r[2], "atom": r[3],
             "col": r[3], "new": r[4], "old": r[5], "pfr": r[6]} for r in rows]


def run(apply: bool) -> dict:
    subject = latest_v26()
    if "_local_only" in subject:
        raise SystemExit(f"GUARD: subject is a _local_only overlay: {subject}")
    sha = _sha256(subject)
    sub = Path(subject).as_posix()
    con = duckdb.connect()
    plan = _arbitrate(con)
    cols = sorted({p["col"] for p in plan})

    # STALE guard: live v26 value must equal the asserted old for every cell
    col_val = " ".join(f"WHEN '{c}' THEN s.{c}" for c in cols)
    vals = ", ".join(f"('{p['player_week']}','{p['col']}',{p['old']},{p['new']})" for p in plan)
    con.execute(f"CREATE TEMP TABLE plan AS SELECT * FROM (VALUES {vals}) t(player_week,col,old,new)")
    check = con.execute(f"""
        SELECT p.player_week, p.col, p.old, p.new,
               TRY_CAST((CASE p.col {col_val} END) AS DOUBLE) live
        FROM plan p LEFT JOIN read_parquet('{sub}') s USING (player_week)""").fetchall()
    stale = [r for r in check if r[4] is None or r[4] != r[2]]
    res = {"wave": WAVE, "mode": "APPLY" if apply else "DRY-RUN", "subject": subject,
           "subject_sha256": sha, "overrides": len(plan),
           "by_atom": {a: sum(1 for p in plan if p["atom"] == a) for a in cols},
           "cells": [{k: p[k] for k in ("player_week", "atom", "old", "new", "pfr")} for p in plan],
           "stale": [list(r) for r in stale]}
    if stale:
        con.close()
        raise SystemExit(f"GUARD: {len(stale)} cells stale vs live subject (adjudicated value "
                         f"changed) -- re-run recon before override: {stale[:5]}")
    if not apply:
        con.close()
        with open(OUT / "PFR_ARBITRATED_DRY_RUN.json", "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2, default=str)
        return res

    ov = {}
    for p in plan:
        ov.setdefault(p["player_week"], {})[p["col"]] = (p["old"], p["new"])
    pf = pq.ParquetFile(subject)
    schema = pf.schema_arrow
    tmp = subject + ".tmp_w60"
    writer = pq.ParquetWriter(tmp, schema, compression="snappy")
    rows_in = rows_out = applied = 0
    for batch in pf.iter_batches(batch_size=BATCH):
        rows_in += batch.num_rows
        pws = batch.column(schema.get_field_index("player_week")).to_pylist()
        hit = {i: pw for i, pw in enumerate(pws) if pw in ov}
        if hit:
            arrays = list(batch.columns)
            for col in {c for i, pw in hit.items() for c in ov[pw]}:
                ci = schema.get_field_index(col)
                vals_l = arrays[ci].to_pylist()
                for i, pw in hit.items():
                    if col in ov[pw]:
                        old, new = ov[pw][col]
                        cur = vals_l[i]
                        if cur is None or float(cur) != old:
                            raise SystemExit(f"GUARD: batch re-check stale at {pw}.{col}: {cur}!={old}")
                        vals_l[i] = type(cur)(new) if cur is not None else new
                        applied += 1
                arrays[ci] = pa.array(vals_l, type=schema.field(ci).type)
            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        rows_out += batch.num_rows
        writer.write_batch(batch)
    writer.close()
    pf.close()

    vcon = duckdb.connect()
    vcon.execute(f"CREATE TEMP TABLE plan AS SELECT * FROM (VALUES {vals}) t(player_week,col,old,new)")
    now_new = vcon.execute(f"""
        SELECT COUNT(*) FROM plan p JOIN read_parquet('{tmp}') s USING (player_week)
        WHERE TRY_CAST((CASE p.col {col_val} END) AS DOUBLE) = p.new""").fetchone()[0]
    out_rows = vcon.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp}')").fetchone()[0]
    vcon.close()
    gates = {"rows": rows_in == rows_out == out_rows, "applied": applied == len(plan),
             "verify_new": now_new == len(plan)}
    res.update(rows_in=rows_in, applied=applied, verify_new=now_new, gates=gates)
    if not all(gates.values()):
        os.remove(tmp)
        res.update(swapped=False, aborted="gate_failure")
        con.close()
        return res

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bk = subject.replace(".parquet", f"_prew60_{stamp}.parquet")
    shutil.copy2(subject, bk)
    os.replace(tmp, subject)

    fcon = facts.connect()
    for p in plan:
        facts.emit_fact("cell_override", fcon, wave_id=WAVE,
            reason=f"PFR third-party scoring attribution ({p['pfr']}) corroborates newspaper "
                   f"against v26 ({p['old']}->{p['new']}); boxscore {p['boxscore_id']}",
            witness="pfr_box_scoring+newspaper_sidecar:scoring_events",
            source_snapshot_id=f"pfr_scoring|subject_sha:{sha}",
            table_name="nfl_player_stats_all", target_key=p["player_week"],
            column_name=p["col"], old_value=repr(p["old"]), new_value=repr(p["new"]))
    fcon.close()
    res.update(swapped=True, backup=bk, facts_emitted=len(plan),
               new_subject_sha256=_sha256(subject))
    con.close()
    with open(OUT / f"PFR_ARBITRATED_RUN_{stamp}.json", "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2, default=str)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        raise SystemExit(
            "FAIL-CLOSED (2026-07-17): direct scoring-cell overrides are unsound -- they "
            "write a leaf stat without recomputing the derived columns that depend on it "
            "(total_tds_scored, scrimmage_tds, pts_*, fantasy points, LAMAR, ranks), leaving rows "
            "internally inconsistent. wave60 was reverted for exactly this. Scoring "
            "corrections must flow through the derivation pipeline, not cell pokes. Dry-run "
            "(no --apply) is kept for reporting the PFR-arbitrated candidate set only.")
    print(json.dumps(run(apply=False), indent=2, default=str))
