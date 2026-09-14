"""
sota_recon/build_position_backfill_newspaper_v26.py -- wave57a: position for wave56 newspaper rows.

The wave56 newspaper promotion inserted 7 player-game rows with position NULL (the
1920s bio rows carry no nfl_position, and the insert builder didn't attach one). A NULL
position keeps those rows out of every position-eligible lane: fpts rescore (OFF_ELIG),
weekly ranks pools, and LAMAR. Backfill from the strongest available witness: the
player's OWN other v26 rows -- same-season modal position, falling back to career modal
(ties broken by count DESC then position ASC, deterministic).

Sets `position` and `nfl_position`; emits cell_override facts per cell.

    python -m scripts.sota_recon.build_position_backfill_newspaper_v26 [--apply]
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

WAVE = "wave57.position_backfill_newspaper"

TARGET_FILTER = (
    "recon_correction_log LIKE 'wave56.newspaper_promotion:%' AND position IS NULL"
)


def _plan(con: duckdb.DuckDBPyConnection, vq: str) -> dict[str, tuple[str, str]]:
    """player_week -> (position, witness) from the player's own rows."""
    rows = con.execute(f"""
        WITH targets AS (
            SELECT player_week, NFL_player_id, year FROM '{vq}' WHERE {TARGET_FILTER}
        ),
        pool AS (
            SELECT w.NFL_player_id, w.year, w.position, COUNT(*) AS n
            FROM '{vq}' w
            JOIN (SELECT DISTINCT NFL_player_id FROM targets) t USING (NFL_player_id)
            WHERE w.position IS NOT NULL
            GROUP BY 1, 2, 3
        ),
        same_year AS (
            SELECT NFL_player_id, year, position, n,
                   ROW_NUMBER() OVER (PARTITION BY NFL_player_id, year
                                      ORDER BY n DESC, position ASC) AS rk
            FROM pool
        ),
        career AS (
            SELECT NFL_player_id, position, SUM(n) AS n,
                   ROW_NUMBER() OVER (PARTITION BY NFL_player_id
                                      ORDER BY SUM(n) DESC, position ASC) AS rk
            FROM pool GROUP BY 1, 2
        )
        SELECT t.player_week,
               COALESCE(sy.position, c.position) AS pos,
               CASE WHEN sy.position IS NOT NULL
                    THEN 'same-year modal from own v26 rows (' || sy.n || ' games ' || CAST(t.year AS INT) || ')'
                    ELSE 'career modal from own v26 rows (' || c.n || ' games)' END AS wit
        FROM targets t
        LEFT JOIN same_year sy ON sy.NFL_player_id = t.NFL_player_id
             AND sy.year = t.year AND sy.rk = 1
        LEFT JOIN career c ON c.NFL_player_id = t.NFL_player_id AND c.rk = 1
        ORDER BY 1""").fetchall()
    return {pw: (pos, wit) for pw, pos, wit in rows if pos}


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    plan = _plan(con, vq)
    n_target = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {TARGET_FILTER}").fetchone()[0]

    if not apply:
        con.close()
        return {"mode": "DRY", "target_rows": n_target, "planned": len(plan),
                "plan": {k: v[0] for k, v in plan.items()}}

    if len(plan) != n_target:
        con.close()
        return {"mode": "APPLY", "gate_pass": False,
                "error": f"plan covers {len(plan)} of {n_target} targets"}

    stamp = utc_stamp()
    con.execute("PRAGMA threads=3")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute(
        "CREATE TEMP TABLE p (player_week VARCHAR, pos VARCHAR)")
    con.executemany("INSERT INTO p VALUES (?, ?)",
                    [(pw, pos) for pw, (pos, _) in plan.items()])

    out_sql = f"""
        SELECT w.* REPLACE (
            CASE WHEN p.player_week IS NOT NULL THEN p.pos ELSE w.position END AS position,
            CASE WHEN p.player_week IS NOT NULL THEN p.pos ELSE w.nfl_position END AS nfl_position,
            CASE WHEN p.player_week IS NOT NULL
                 THEN w.recon_correction_log || ',{WAVE}'
                 ELSE w.recon_correction_log END AS recon_correction_log)
        FROM '{vq}' w LEFT JOIN p ON w.player_week = p.player_week"""

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_posbf.parquet")
    before_n = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    rb = con.execute(out_sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), rb.schema)
    for batch in rb:
        writer.write_batch(batch)
    writer.close()

    tq = tmp.as_posix()
    after_n = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_target = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {TARGET_FILTER}").fetchone()[0]
    applied = dict(con.execute(f"""
        SELECT player_week, position FROM '{tq}'
        WHERE recon_correction_log LIKE '%{WAVE}%'""").fetchall())
    # nothing else may change: every non-target row byte-identical on a broad checksum
    chk = """SELECT COUNT(*), SUM(hash(COALESCE(player_week,'') || '|' ||
             COALESCE(CAST(fpts_4pt_half AS VARCHAR),'') || '|' ||
             COALESCE(CAST(rushing_yards AS VARCHAR),'') || '|' ||
             COALESCE(CAST(passing_interceptions AS VARCHAR),'') || '|' ||
             COALESCE(position,''))) FROM '{q}' WHERE NOT ({f})"""
    chk_before = con.execute(chk.format(q=vq, f=TARGET_FILTER)).fetchone()
    chk_after = con.execute(
        chk.format(q=tq, f=f"recon_correction_log LIKE '%{WAVE}%'")).fetchone()

    gate = (
        after_n == before_n
        and after_target == 0
        and applied == {pw: pos for pw, (pos, _) in plan.items()}
        and chk_before == chk_after
    )
    res = {"mode": "APPLY", "rows": [before_n, after_n],
           "targets_after": after_target, "applied": applied,
           "checksum_match": chk_before == chk_after, "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        for pw, (pos, wit) in plan.items():
            for col in ("position", "nfl_position"):
                facts.emit_fact("cell_override", con=fc,
                                table_name="nfl_player_stats_all",
                                target_key=pw, column_name=col,
                                old_value="NULL", new_value=pos,
                                wave_id=WAVE,
                                reason="wave56 newspaper insert rows lacked position; "
                                       "backfilled from player's own row population",
                                witness=wit, source_snapshot_id=snap)
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew57a_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res.update(backup=bk.name, swapped=True)
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    print(run(apply=ap.parse_args().apply))
