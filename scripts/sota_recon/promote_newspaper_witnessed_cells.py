"""
sota_recon/promote_newspaper_witnessed_cells.py -- wave58: Round-1 newspaper witnessed-cell
FILL. The ONLY sanctioned path from newspaper evidence into v26 under the sidecar-witness
architecture (2026-07-17; bulk promotion is retired and fail-closed).

Input is the gate-clean candidate ledger written by the recon lane triage:
  sota_recon_master/newspaper_sidecar/ROUND1_FILL_CANDIDATES.csv
(89 cells / 79 player-weeks / 66 games; every candidate passed G1-G9, carries a citation,
and its game has no over-claims, no score conflicts, no holds, no disagreements.)

Doctrine (unchanged from wave56): FILL previously-NULL cells only. No inserts this round
(all new-row candidates are the overlay-unmatched hold queue). No overwrites ever -- a
non-NULL cell that differs from the ledger ABORTS the whole run fail-closed.

Gates: subject sha256 must equal the sha the ledger was adjudicated against; row count
unchanged; fills == plan; per-cell recount proof on the output before swap; timestamped
backup; cell_override facts with citations; post-run expectation = recon lane G7
fill_signal drops by exactly the applied count.

    python -m scripts.sota_recon.promote_newspaper_witnessed_cells            # DRY RUN
    python -m scripts.sota_recon.promote_newspaper_witnessed_cells --apply
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
from .newspaper_witness_common import ATOM_TO_V26_COL, NEWSPAPER_BUNDLE_DIR
from .sources import DATA_LAKE, latest_v26

WAVE = "wave58.newspaper_witnessed_fill_r1"
OUT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "newspaper_sidecar"
LEDGER = OUT / "ROUND1_FILL_CANDIDATES.csv"
# sha256 of the subject the ledger was adjudicated against (recon run 2026-07-17)
EXPECTED_SUBJECT_SHA = "165ef7b92f1723b969817b05644aba90a3f4f20a663f9943036bc2897850aa76"
BATCH = 131_072


def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_ledger(con) -> list[dict]:
    rows = con.execute(f"""
        SELECT player_week, NFL_player_id, boxscore_id, atom,
               TRY_CAST(np_value AS DOUBLE) v, confidence_bar, source_documents
        FROM read_csv_auto('{LEDGER.as_posix()}') ORDER BY player_week, atom""").fetchall()
    cands = []
    seen = set()
    for pw, pid, box, atom, v, conf, src in rows:
        if atom not in ATOM_TO_V26_COL:
            raise SystemExit(f"GUARD: ledger atom '{atom}' has no sanctioned v26 column")
        if v is None:
            raise SystemExit(f"GUARD: non-numeric ledger value for {pw}/{atom}")
        if (pw, atom) in seen:
            raise SystemExit(f"GUARD: duplicate ledger cell {pw}/{atom}")
        seen.add((pw, atom))
        cands.append({"player_week": pw, "NFL_player_id": pid, "boxscore_id": box,
                      "atom": atom, "col": ATOM_TO_V26_COL[atom], "value": v,
                      "confidence": conf, "citation": src})
    return cands


def run(apply: bool) -> dict:
    subject = latest_v26()
    if "_local_only" in subject:
        raise SystemExit(f"GUARD: subject resolved to a _local_only overlay: {subject}")
    sha = _sha256(subject)
    if sha != EXPECTED_SUBJECT_SHA:
        raise SystemExit(
            f"GUARD: subject sha {sha[:12]} != ledger adjudication sha "
            f"{EXPECTED_SUBJECT_SHA[:12]} -- re-run recon_newspaper_sidecar and rebuild "
            f"the candidate ledger before promoting")

    con = duckdb.connect()
    cands = _load_ledger(con)
    cols = sorted({c["col"] for c in cands})
    sub = Path(subject).as_posix()

    # runtime re-verification against the live subject (fail-closed on any conflict)
    con.execute(f"""
        CREATE TEMP TABLE cand AS
        SELECT * FROM (VALUES {', '.join(
            f"('{c['player_week']}', '{c['col']}', {c['value']})" for c in cands)})
        t(player_week, col, v)""")
    col_val = " ".join(f"WHEN '{c}' THEN s.{c}" for c in cols)
    verify = con.execute(f"""
        SELECT c.player_week, c.col, c.v,
               CASE WHEN s.player_week IS NULL THEN 'missing_row'
                    WHEN (CASE c.col {col_val} END) IS NULL THEN 'fillable'
                    WHEN TRY_CAST((CASE c.col {col_val} END) AS DOUBLE) = c.v THEN 'already_equal'
                    ELSE 'CONFLICT' END st
        FROM cand c LEFT JOIN read_parquet('{sub}') s USING (player_week)""").fetchall()
    by_state: dict[str, int] = {}
    for _, _, _, st in verify:
        by_state[st] = by_state.get(st, 0) + 1
    if by_state.get("CONFLICT") or by_state.get("missing_row"):
        bad = [r for r in verify if r[3] in ("CONFLICT", "missing_row")]
        raise SystemExit(f"GUARD: ledger no longer applies cleanly: {bad[:10]}")
    fill_keys = {(r[0], r[1]) for r in verify if r[3] == "fillable"}
    plan = [c for c in cands if (c["player_week"], c["col"]) in fill_keys]

    # close the verification connection NOW: DuckDB caches file handles per connection,
    # and an open handle on the subject makes the final os.replace fail on Windows
    con.close()

    res = {"wave": WAVE, "mode": "APPLY" if apply else "DRY-RUN", "subject": subject,
           "subject_sha256": sha, "ledger": str(LEDGER), "candidates": len(cands),
           "verify_states": by_state, "planned_fills": len(plan),
           "planned_by_column": {c: sum(1 for p in plan if p["col"] == c) for c in cols}}
    if not apply:
        with open(OUT / "ROUND1_PROMOTION_DRY_RUN.json", "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2)
        return res

    # --- APPLY: stream-rewrite, touching only planned cells --------------------------
    fills = {}
    for p in plan:
        fills.setdefault(p["player_week"], {})[p["col"]] = p["value"]
    pf = pq.ParquetFile(subject)
    schema = pf.schema_arrow
    tmp = subject + ".tmp_w58"
    writer = pq.ParquetWriter(tmp, schema, compression="snappy")
    rows_in = rows_out = applied = 0
    for batch in pf.iter_batches(batch_size=BATCH):
        rows_in += batch.num_rows
        pws = batch.column(schema.get_field_index("player_week")).to_pylist()
        hit = {i: fills[pw] for i, pw in enumerate(pws) if pw in fills}
        if hit:
            arrays = batch.columns
            new_arrays = list(arrays)
            for col in {c for f in hit.values() for c in f}:
                ci = schema.get_field_index(col)
                vals = arrays[ci].to_pylist()
                for i, f in hit.items():
                    if col in f:
                        if vals[i] is not None:
                            raise SystemExit(f"GUARD: batch re-check found non-NULL at "
                                             f"{pws[i]}.{col}")
                        vals[i] = f[col]
                        applied += 1
                new_arrays[ci] = pa.array(vals, type=schema.field(ci).type)
            batch = pa.RecordBatch.from_arrays(new_arrays, schema=schema)
        rows_out += batch.num_rows
        writer.write_batch(batch)
    writer.close()
    pf.close()  # release the reader's handle on the subject or os.replace fails on Windows

    gate_rows = rows_in == rows_out
    gate_fills = applied == len(plan)
    # per-cell recount proof on the output file
    vcon = duckdb.connect()
    vcon.execute(f"""CREATE TEMP TABLE cand AS
        SELECT * FROM (VALUES {', '.join(
            f"('{p['player_week']}', '{p['col']}', {p['value']})" for p in plan)})
        t(player_week, col, v)""")
    recount = vcon.execute(f"""
        SELECT COUNT(*) FROM cand c
        JOIN read_parquet('{tmp}') s USING (player_week)
        WHERE TRY_CAST((CASE c.col {col_val} END) AS DOUBLE) = c.v""").fetchone()[0]
    out_rows = vcon.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp}')").fetchone()[0]
    vcon.close()
    gate_recount = recount == len(plan) and out_rows == rows_in

    res.update(rows_in=rows_in, rows_out=rows_out, applied=applied,
               recount_proof=recount,
               gates={"rows": gate_rows, "fills": gate_fills, "recount": gate_recount})
    if not (gate_rows and gate_fills and gate_recount):
        os.remove(tmp)
        res.update(swapped=False, aborted="gate_failure")
        return res

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bk = subject.replace(".parquet", f"_prew58_{stamp}.parquet")
    shutil.copy2(subject, bk)
    os.replace(tmp, subject)

    fcon = facts.connect()
    for p in plan:
        facts.emit_fact(
            "cell_override", fcon,
            wave_id=WAVE,
            reason=f"newspaper witnessed-cell fill r1 ({p['confidence']} confidence, "
                   f"boxscore {p['boxscore_id']})",
            witness="newspaper_sidecar:weekly_player_stat_cells",
            source_snapshot_id=f"newspaper_bundle:{Path(NEWSPAPER_BUNDLE_DIR).name}|"
                               f"subject_sha:{sha}",
            table_name="nfl_player_stats_all",
            target_key=p["player_week"],
            column_name=p["col"],
            old_value=None,
            new_value=repr(p["value"]),
        )
    fcon.close()
    res.update(swapped=True, backup=bk, facts_emitted=len(plan),
               new_subject_sha256=_sha256(subject))
    with open(OUT / f"ROUND1_PROMOTION_RUN_{stamp}.json", "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
