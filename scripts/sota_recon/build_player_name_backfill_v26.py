"""
sota_recon/build_player_name_backfill_v26.py -- wave57: display names for insert-wave rows.

19,472 weekly rows (waves 44/49/50/51/56 inserts) carry no `player` display name -- the
frontend renders them as "Unknown" (Joe's sighting: the #2 worst-game-ever row). All are
bio-linked by construction; the name is a mechanical join. Fills only NULL/blank cells:
`player` from bio.player, and headshot_url opportunistically where bio has one.

Root fix in the same commit: the insert builders attach player at insert.

    python -m scripts.sota_recon.build_player_name_backfill_v26 [--apply]
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .sources import latest_v26, PLAYER_BIO
from .recon_common import utc_stamp

WAVE = "wave57.player_name_backfill"
QUEUE_TAG = "player_name_unresolved_queue"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"

MISSING = "(player IS NULL OR TRIM(player) = '')"


def _bio_path() -> str:
    return Path(PLAYER_BIO.path).as_posix() if hasattr(PLAYER_BIO, "path") else str(PLAYER_BIO)


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    bio = _bio_path()
    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    con.execute("PRAGMA threads=2")

    bio_cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{bio}'").fetchall()}
    wk_cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    fill_headshot = "headshot_url" in bio_cols and "headshot_url" in wk_cols

    # dup-key law: player_week-keyed writes exclude dup keys (verified 0 overlap live)
    con.execute(f"""CREATE TEMP TABLE dupkeys AS
        SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*) > 1""")
    con.execute(f"""CREATE TEMP TABLE plan AS
        SELECT w.player_week, b.player AS new_name,
               {'b.headshot_url AS new_headshot' if fill_headshot else 'NULL AS new_headshot'}
        FROM '{vq}' w
        JOIN '{bio}' b ON b.NFL_player_id = w.NFL_player_id
        WHERE {MISSING.replace('player', 'w.player')}
          AND b.player IS NOT NULL AND TRIM(b.player) <> ''
          AND w.player_week NOT IN (SELECT player_week FROM dupkeys)""")
    con.execute(f"""CREATE TEMP TABLE queue AS
        SELECT w.player_week, w.NFL_player_id FROM '{vq}' w
        WHERE {MISSING.replace('player', 'w.player')}
          AND w.player_week NOT IN (SELECT player_week FROM plan)""")
    n_plan = con.execute("SELECT COUNT(*) FROM plan").fetchone()[0]
    n_queue = con.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    n_dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT player_week FROM plan GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    res_common = {"planned_fills": n_plan, "queued": n_queue,
                  "dup_keys_in_plan": n_dup, "fill_headshot": fill_headshot}
    if not apply:
        con.close()
        return {"mode": "DRY", **res_common}
    if n_dup:
        con.close()
        return {"mode": "APPLY", "gate_pass": False, **res_common,
                "error": "plan has duplicate player_week targets"}

    stamp = utc_stamp()
    os.makedirs(SWEEPS, exist_ok=True)
    queue_csv = os.path.join(SWEEPS, "wave57_player_name_queue.csv")
    con.execute(f"COPY (SELECT * FROM queue ORDER BY player_week) TO "
                f"'{Path(queue_csv).as_posix()}' (HEADER)")

    names = dict(con.execute("SELECT player_week, new_name FROM plan").fetchall())
    heads = {k: h for k, _, h in con.execute(
        "SELECT player_week, new_name, new_headshot FROM plan").fetchall() if h}
    before_n = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    before_dups = con.execute(
        f"SELECT COUNT(*) FROM (SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    before_missing = con.execute(
        f"SELECT COUNT(*) FROM '{vq}' WHERE {MISSING}").fetchone()[0]

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_namebf.parquet")
    pf = pq.ParquetFile(v26)
    writer = None
    after = 0
    try:
        for batch in pf.iter_batches(batch_size=50000):
            bn = batch.schema.names
            pw_i, pl_i, log_i = (bn.index("player_week"), bn.index("player"),
                                 bn.index("recon_correction_log"))
            pw = batch.column(pw_i).to_pylist()
            pl = batch.column(pl_i).to_pylist()
            log = batch.column(log_i).to_pylist()
            new_pl, new_log = [], []
            hs = batch.column(bn.index("headshot_url")).to_pylist() if fill_headshot else None
            for j, k in enumerate(pw):
                nm = names.get(k)
                if nm is not None and (pl[j] is None or str(pl[j]).strip() == ""):
                    new_pl.append(nm)
                    new_log.append((log[j] + "," if log[j] else "") + WAVE)
                    if hs is not None and hs[j] is None and k in heads:
                        hs[j] = heads[k]
                else:
                    new_pl.append(pl[j])
                    new_log.append(log[j])
            arrays = list(batch.columns)
            arrays[pl_i] = pa.array(new_pl, type=batch.schema.field(pl_i).type)
            arrays[log_i] = pa.array(new_log, type=batch.schema.field(log_i).type)
            if hs is not None:
                hi = bn.index("headshot_url")
                arrays[hi] = pa.array(hs, type=batch.schema.field(hi).type)
            out = pa.record_batch(arrays, schema=batch.schema)
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), batch.schema)
            writer.write_batch(out)
            after += out.num_rows
    finally:
        if writer is not None:
            writer.close()
        pf.close()

    tq = tmp.as_posix()
    after_missing = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {MISSING}").fetchone()[0]
    after_dups = con.execute(
        f"SELECT COUNT(*) FROM (SELECT player_week FROM '{tq}' GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    n_stamped = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE recon_correction_log LIKE '%{WAVE}%'"
    ).fetchone()[0]
    pollard = con.execute(
        f"SELECT player FROM '{tq}' WHERE player_week = 'PollFr21_1920_12'").fetchone()

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
        and after == before_n
        and after_dups == before_dups
        and n_stamped == n_plan
        and after_missing == n_queue
        and before_missing == n_plan + n_queue
        and (pollard or [None])[0] == "Fritz Pollard"
    )
    res = {"mode": "APPLY", **res_common, "rows": [before_n, after],
           "missing": [before_missing, after_missing], "stamped": n_stamped,
           "dup_keys": [before_dups, after_dups],
           "pollard_name": (pollard or [None])[0],
           "golden": f"{golden['passed']}/{golden['total']}", "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        for pw_key, nm in names.items():
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw_key, column_name="player",
                            old_value="NULL", new_value=str(nm), wave_id=WAVE,
                            reason="insert-wave rows lacked display name; frontend "
                                   "rendered 'Unknown'",
                            witness="player_bio.player via NFL_player_id",
                            source_snapshot_id=snap)
        for pw_key, _nfl in con.execute("SELECT * FROM queue").fetchall():
            facts.emit_fact("row_tag", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw_key, tag=QUEUE_TAG, wave_id=WAVE,
                            reason="no bio name or dup-key row",
                            witness=f"queue csv {os.path.basename(queue_csv)}",
                            source_snapshot_id=snap)
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew57h_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res.update(backup=bk.name, queue_csv=queue_csv, swapped=True)
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    print(run(apply=ap.parse_args().apply))
