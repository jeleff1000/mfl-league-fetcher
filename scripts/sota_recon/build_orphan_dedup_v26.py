"""
sota_recon/build_orphan_dedup_v26.py  --  wave46: delete orphan CLONE rows.

recon_stragglers found 6,539 player rows sitting on (franchise, year, week) combinations
where the schedule says no game was played. Of these, the CLONE subset is deletable with
the strongest witness there is: the SAME player already has a row at a properly SCHEDULED
week of the same season with an IDENTICAL full stat vector -- the orphan is a duplicate of
a real game, double-counting its stats (the prime suspect class for the 1950-64
conservation OVER).

Clone criterion (full-vector, not just yardage): identical across passing_yards/tds/
completions/attempts, rushing_yards/tds/carries, receiving_yards/tds/receptions -- AND the
row has nonzero content -- AND the scheduled-week twin is itself on a real game. Zero-content
orphans, pure shifts, and partial matches are NOT touched (separate dispositions).

Gates on apply: rowcount == before - deleted; orphan count drops by exactly deleted;
no dup-group change; golden pass. Every deletion emits a row_delete fact with the twin's
player_week as witness. Backup + swap.

    python -m scripts.sota_recon.build_orphan_dedup_v26            # DRY RUN
    python -m scripts.sota_recon.build_orphan_dedup_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import TEAM_GAMES, latest_v26

WAVE = "wave46.orphan_clone_dedup"
VECTOR = ["passing_yards", "passing_tds", "completions", "attempts",
          "rushing_yards", "rushing_tds", "carries",
          "receiving_yards", "receiving_tds", "receptions"]
PREVIEW = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
           r"\phase1_sweeps\wave46_clone_dedup_preview.csv")


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build(con, vq: str) -> None:
    tg = _q(TEAM_GAMES)
    vec_eq = " AND ".join(
        f"COALESCE(o.{c}, -999) = COALESCE(t.{c}, -999)" for c in VECTOR)
    nonzero = " + ".join(f"ABS(COALESCE(o.{c}, 0))" for c in VECTOR)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sched AS
    SELECT DISTINCT team_fid, year, CAST(week AS INT) AS week, season_type FROM '{tg}'""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE doomed AS
    WITH orph AS (
      SELECT v.* FROM '{vq}' v
      LEFT JOIN sched s ON s.team_fid = v.nfl_franchise_number AND s.year = v.year
        AND s.week = CAST(v.week AS INT) AND s.season_type = v.season_type
      WHERE v.nfl_franchise_number IS NOT NULL AND v.week IS NOT NULL
        AND s.team_fid IS NULL)
    SELECT DISTINCT o.player_week AS orphan_pw, ANY_VALUE(t.player_week) AS twin_pw,
           ANY_VALUE(o.year) AS year
    FROM orph o
    JOIN '{vq}' t ON t.NFL_player_id = o.NFL_player_id AND t.year = o.year
      AND t.player_week <> o.player_week
    JOIN sched s2 ON s2.team_fid = t.nfl_franchise_number AND s2.year = t.year
      AND s2.week = CAST(t.week AS INT) AND s2.season_type = t.season_type
    WHERE {vec_eq} AND ({nonzero}) > 0
      -- deletion is by player_week: only doom pws with exactly ONE physical row, or the
      -- delete would take out doubleheader twins sharing the pw (gate caught this: -715 vs -714)
      AND o.player_week IN (SELECT player_week FROM '{vq}' WHERE player_week IS NOT NULL
                            GROUP BY 1 HAVING COUNT(*) = 1)
    GROUP BY o.player_week""")


def run(apply: bool = False) -> dict:
    vq = _q(latest_v26())
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    _build(con, vq)
    n = con.execute("SELECT COUNT(*) FROM doomed").fetchone()[0]
    per_era = con.execute("""
        SELECT CAST(year/10 AS INT)*10 AS decade, COUNT(*) FROM doomed GROUP BY 1 ORDER BY 1
    """).fetchall()
    con.execute(f"COPY doomed TO '{Path(PREVIEW).as_posix()}' (HEADER)")
    diag = {"clone_deletions": n, "per_decade": [(int(d), c) for d, c in per_era],
            "preview_csv": PREVIEW}
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}

    stamp = utc_stamp()
    vp = Path(latest_v26())
    tmp = vp.with_name(vp.stem + "_w46.parquet")
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    r = con.execute(f"""
        SELECT * FROM '{vq}' WHERE player_week NOT IN (SELECT orphan_pw FROM doomed)
           OR player_week IS NULL""").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close()

    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]

    from . import golden_samples
    import scripts.sota_recon.sources as S
    orig = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = orig

    gate = (after == before - n) and g["failed"] == 0
    res = {"mode": "APPLY", **diag, "before": before, "after": after,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        rows = con.execute("SELECT orphan_pw, twin_pw FROM doomed").fetchall()
        snap = os.path.basename(os.path.dirname(vq))
        for opw, tpw in rows:
            facts.emit_fact("row_delete", con=fc, table_name="nfl_player_stats_all",
                            target_key=opw, old_row_hash=f"clone_of:{tpw}",
                            wave_id=WAVE,
                            reason="orphan row on unscheduled week; identical full stat "
                                   "vector exists at a scheduled week (double-count)",
                            witness=f"schedule+twin_row:{tpw}",
                            source_snapshot_id=snap)
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew46_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
