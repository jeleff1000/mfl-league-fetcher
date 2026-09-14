"""
sota_recon/build_float_key_repair_v26.py -- wave57: repair float-year player_week keys.

Waves 44/49/50/51 built player_week as `id || '_' || year || '_' || week` with a DOUBLE
year, minting 23,553 keys like 'KingFa20_1947.0_15'. player_week is THE join key
(frontend, witness lanes, dedup), so these rows silently miss every join. Worse, the
insert guards' NOT-EXISTS checks used the same malformed key, so 4,096 rows whose true
row already existed slipped past the guard as logical duplicates (same player, team,
game -- twin proven 1:1, zero team mismatches, zero DH involvement).

Buckets (closed, exact-count gated):
  RENAME        no true-key twin -> rewrite the key
  MERGE_DELETE  twin exists, float row's compare-set adds nothing over the twin
                (atom-complementary cells filled into the twin first) -> delete float
  QUEUE         twin exists with conflicting non-null compare-set values -> untouched,
                parked with evidence (never guess)

Facts: cell_override per rename + per filled cell; row_delete (row-hash pinned) per
delete; row_tag per queued row. Deleted rows dumped to a parquet sidecar before swap.
Root fix (CAST year to INT) applied to all four insert builders in the same commit.

    python -m scripts.sota_recon.build_float_key_repair_v26 [--apply]
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
from .build_missing_row_backfill_v26 import _all_v26_atoms

WAVE_RENAME = "wave57.float_key_repair"
WAVE_DELETE = "wave57.float_key_dup_delete"
WAVE_ABSORB = "wave57.float_key_dup_absorbed"
QUEUE_TAG = "float_key_conflict_queue"

PAT = "_[0-9]{4}[.]0_"
FIX = ("replace(player_week, '_' || CAST(year AS INT) || '.0_', "
       "'_' || CAST(year AS INT) || '_')")
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"

# identity context must AGREE or the pair is not a safe merge -> queue
IDENTITY_COLS = ["game_date", "season_type", "nfl_team", "opponent_nfl_team",
                 "nfl_franchise_number", "opponent_nfl_franchise_number"]
EXTRA_COMPARE = ["is_starter", "starter_position"]


def _compare_cols(con: duckdb.DuckDBPyConnection, vq: str) -> list[str]:
    have = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    cols = [c for c in _all_v26_atoms() if c in have]
    cols += [c for c in EXTRA_COMPARE + IDENTITY_COLS if c in have and c not in cols]
    return cols


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    _spill = os.path.join(os.path.dirname(v26), f".duckspill_{utc_stamp()}")
    os.makedirs(_spill, exist_ok=True)
    con.execute(f"SET temp_directory='{Path(_spill).as_posix()}'")
    cols = _compare_cols(con, vq)
    types = dict(con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall() and [
        (r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()])

    con.execute(f"""CREATE TEMP TABLE bad AS
        SELECT player_week AS old_key, {FIX} AS new_key
        FROM '{vq}' WHERE regexp_matches(player_week, '{PAT}')""")
    n_bad = con.execute("SELECT COUNT(*) FROM bad").fetchone()[0]
    con.execute(f"""CREATE TEMP TABLE coll AS
        SELECT b.old_key, b.new_key FROM bad b
        JOIN '{vq}' w ON w.player_week = b.new_key""")
    n_coll = con.execute("SELECT COUNT(*) FROM coll").fetchone()[0]

    def _nan_safe(side: str, c: str) -> str:
        ref = f'{side}."{c}"'
        if types.get(c, "").upper() in ("DOUBLE", "FLOAT", "REAL"):
            return f"(CASE WHEN isnan({ref}) THEN NULL ELSE {ref} END)"
        return ref

    conflict_terms, fill_terms = [], []
    for c in cols:
        f, t = _nan_safe("f", c), _nan_safe("t", c)
        conflict_terms.append(
            f"CASE WHEN {f} IS NOT NULL AND {t} IS NOT NULL AND {f} IS DISTINCT FROM {t} "
            f"THEN '{c},' ELSE '' END")
        fill_terms.append(
            f"CASE WHEN {t} IS NULL AND {f} IS NOT NULL THEN '{c},' ELSE '' END")
    con.execute(f"""CREATE TEMP TABLE pairs AS
        SELECT c.old_key, c.new_key,
               {" || ".join(conflict_terms)} AS conflicts,
               {" || ".join(fill_terms)} AS fills
        FROM coll c
        JOIN '{vq}' f ON f.player_week = c.old_key
        JOIN '{vq}' t ON t.player_week = c.new_key""")
    queue = con.execute("SELECT old_key, new_key, conflicts FROM pairs "
                        "WHERE conflicts <> '' ORDER BY old_key").fetchall()
    merge = con.execute("SELECT old_key, new_key, fills FROM pairs "
                        "WHERE conflicts = '' ORDER BY old_key").fetchall()
    n_rename = n_bad - n_coll

    # per-cell fill plan: (new_key, col, value from float row) -- set-based, one
    # parquet scan for the float rows, then per-column pulls from the temp table
    con.execute(f"""CREATE TEMP TABLE floatrows AS
        SELECT w.* FROM '{vq}' w
        JOIN pairs p ON p.old_key = w.player_week AND p.conflicts = ''""")
    fill_plan: list[tuple[str, str, object]] = []
    fill_cols_any = sorted({x for _, _, fills in merge for x in fills.split(",") if x})
    for c in fill_cols_any:
        rows = con.execute(f"""
            SELECT p.new_key, {_nan_safe("f", c)}
            FROM pairs p JOIN floatrows f ON f.player_week = p.old_key
            WHERE p.conflicts = '' AND (',' || p.fills) LIKE '%,{c},%'""").fetchall()
        fill_plan.extend((k, c, val) for k, val in rows if val is not None)

    res_common = {
        "float_keys": n_bad, "collisions": n_coll, "renames": n_rename,
        "merge_deletes": len(merge), "queued_conflicts": len(queue),
        "fill_cells": len(fill_plan),
        "fill_cols": sorted({c for _, c, _ in fill_plan}),
    }
    if not apply:
        con.close()
        shutil.rmtree(_spill, ignore_errors=True)
        return {"mode": "DRY", **res_common,
                "queue_sample": queue[:5]}

    stamp = utc_stamp()
    os.makedirs(SWEEPS, exist_ok=True)
    queue_csv = os.path.join(SWEEPS, "wave57_float_key_conflict_queue.csv")
    con.execute(f"""COPY (SELECT * FROM pairs WHERE conflicts <> '' ORDER BY old_key)
                    TO '{Path(queue_csv).as_posix()}' (HEADER)""")

    vp = Path(v26)
    deleted_dump = vp.with_name(f"wave57_deleted_float_dups_{stamp}.parquet")
    con.execute("CREATE TEMP TABLE dels AS SELECT old_key, new_key FROM pairs WHERE conflicts = ''")

    con.execute("""CREATE TEMP TABLE renames AS
        SELECT old_key, new_key FROM bad
        WHERE old_key NOT IN (SELECT old_key FROM coll)""")
    ren = dict(con.execute("SELECT old_key, new_key FROM renames").fetchall())
    del_set = {k for (k,) in con.execute("SELECT old_key FROM dels").fetchall()}
    absorb_set = {k for (k,) in con.execute("SELECT DISTINCT new_key FROM dels").fetchall()}
    fills_by_col: dict[str, dict[str, object]] = {}
    for k, c, vval in fill_plan:
        fills_by_col.setdefault(c, {})[k] = vval

    before_n = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    before_dups = con.execute(
        f"SELECT COUNT(*) FROM (SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]

    # DuckDB join+wide-write wedges on this table (runbook lesson); stream the parquet
    # through pyarrow and apply the small plan dicts per batch instead. The deleted-row
    # sidecar dump + row hashes come from the same stream (a wide DuckDB semi-join OOMs).
    import hashlib
    import json as _json
    import pyarrow as pa
    tmp = vp.with_name(vp.stem + "_keyrepair.parquet")
    pf = pq.ParquetFile(v26)
    writer = None
    del_writer = None
    del_hashes: dict[str, str] = {}
    try:
        for batch in pf.iter_batches(batch_size=50000):
            names = batch.schema.names
            pw_i = names.index("player_week")
            log_i = names.index("recon_correction_log")
            pw = batch.column(pw_i).to_pylist()
            keep = [k not in del_set for k in pw]
            if not all(keep):
                dropped = batch.filter(pa.array([not k for k in keep]))
                if del_writer is None:
                    del_writer = pq.ParquetWriter(str(deleted_dump), batch.schema)
                del_writer.write_batch(dropped)
                for row in dropped.to_pylist():
                    payload = _json.dumps(row, sort_keys=True, default=str)
                    del_hashes[row["player_week"]] = hashlib.md5(
                        payload.encode()).hexdigest()
            log = batch.column(log_i).to_pylist()
            new_pw, new_log = [], []
            for k, lg in zip(pw, log):
                if k in ren:
                    new_pw.append(ren[k])
                    new_log.append((lg + "," if lg else "") + WAVE_RENAME)
                else:
                    new_pw.append(k)
                    if k in absorb_set:
                        new_log.append((lg + "," if lg else "") + WAVE_ABSORB)
                    else:
                        new_log.append(lg)
            arrays = list(batch.columns)
            arrays[pw_i] = pa.array(new_pw, type=batch.schema.field(pw_i).type)
            arrays[log_i] = pa.array(new_log, type=batch.schema.field(log_i).type)
            for c, kv in fills_by_col.items():
                ci = names.index(c)
                col = batch.column(ci).to_pylist()
                changed = False
                for j, k in enumerate(pw):
                    if col[j] is None and k in kv:
                        col[j] = kv[k]
                        changed = True
                if changed:
                    arrays[ci] = pa.array(col, type=batch.schema.field(ci).type)
            out = pa.record_batch(arrays, schema=batch.schema)
            if not all(keep):
                out = out.filter(pa.array(keep))
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), batch.schema)
            writer.write_batch(out)
    finally:
        if writer is not None:
            writer.close()
        if del_writer is not None:
            del_writer.close()
        pf.close()

    tq = tmp.as_posix()
    after_n = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_float = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE regexp_matches(player_week, '{PAT}')").fetchone()[0]
    after_dups = con.execute(
        f"SELECT COUNT(*) FROM (SELECT player_week FROM '{tq}' GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    n_stamp_rename = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE recon_correction_log LIKE '%{WAVE_RENAME}%'"
    ).fetchone()[0]
    fill_ok = True
    for k, c, vval in fill_plan:
        got = con.execute(f'SELECT "{c}" FROM \'{tq}\' WHERE player_week = ?', [k]).fetchone()[0]
        if str(got) != str(vval):
            fill_ok = False
            break

    from . import golden_samples
    import scripts.sota_recon.sources as S
    old_latest, old_g = S.latest_v26, golden_samples.latest_v26
    S.latest_v26 = golden_samples.latest_v26 = lambda: str(tmp)
    try:
        golden = golden_samples.run()
    finally:
        S.latest_v26, golden_samples.latest_v26 = old_latest, old_g

    gate = (
        golden["failed"] == 0
        and after_n == before_n - len(merge)
        and after_float == len(queue)
        and after_dups == before_dups
        and n_stamp_rename == n_rename
        and fill_ok
        and len(del_hashes) == len(merge)
    )
    res = {"mode": "APPLY", **res_common,
           "rows": [before_n, after_n], "float_keys_after": after_float,
           "dup_keys": [before_dups, after_dups], "rename_stamped": n_stamp_rename,
           "fill_verified": fill_ok, "golden": f"{golden['passed']}/{golden['total']}",
           "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        for old_key, new_key in con.execute("SELECT old_key, new_key FROM renames").fetchall():
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=old_key, column_name="player_week",
                            old_value=old_key, new_value=new_key, wave_id=WAVE_RENAME,
                            reason="float-year key minted by insert-wave concat bug",
                            witness="year column of the row; no true-key twin exists",
                            source_snapshot_id=snap)
        for old_key, new_key, _fills in merge:
            facts.emit_fact("row_delete", con=fc, table_name="nfl_player_stats_all",
                            target_key=old_key, old_row_hash=del_hashes[old_key],
                            wave_id=WAVE_DELETE,
                            reason="guard checked malformed key and re-inserted an "
                                   "existing game; twin proven same player/team/game, "
                                   "compare-set adds nothing beyond filled cells",
                            witness=f"true-key twin {new_key} (team match, 1:1)",
                            source_snapshot_id=snap)
        for new_key, c, vval in fill_plan:
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=new_key, column_name=c,
                            old_value="NULL", new_value=str(vval), wave_id=WAVE_DELETE,
                            reason="cell absorbed from deleted float-key duplicate",
                            witness="float-key twin row (see row_delete fact)",
                            source_snapshot_id=snap)
        for old_key, new_key, conflicts in queue:
            facts.emit_fact("row_tag", con=fc, table_name="nfl_player_stats_all",
                            target_key=old_key, tag=QUEUE_TAG, wave_id=WAVE_RENAME,
                            reason=f"twin {new_key} conflicts on: {conflicts.rstrip(',')}",
                            witness=f"queue csv {os.path.basename(queue_csv)}",
                            source_snapshot_id=snap)
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew57e_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res.update(backup=bk.name, deleted_dump=deleted_dump.name,
                   queue_csv=queue_csv, swapped=True)
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    shutil.rmtree(_spill, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    print(run(apply=ap.parse_args().apply))
