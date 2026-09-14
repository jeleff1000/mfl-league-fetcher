"""
sota_recon/build_pbp_backfill_v26.py  --  wave47: fill 1978-1998 NULL cells from merged PBP.

Joe's directive: more stats are derivable back to 1978 from PBP. This wave fills the
mechanical set (EPA aggregation is a separate wave pending nflverse-semantics overlap
calibration):

    receiving_target_interceptions   intended receiver on INT plays
    sack_yards_lost                  -yards_gained summed over the passer's sacks
    punt_return_long / kickoff_return_long   MAX(return_yards) per returner
    punt_long                        MAX(kick_distance) per punter

SELF-LICENSING: each derivation is validated on 1999-2015 against v26's existing non-null
values first; a column fills 1978-98 ONLY if overlap agreement >= 95%. NULL cells only --
existing values are never overwritten (COALESCE keeps v26; disagreements are reported, not
applied). ID mapping via the rollup's proven pbp_player_id -> NFL_player_id pairs
(unambiguous mappings only).

Gates on apply: row count unchanged; zero non-null cells changed; per-column filled == plan;
golden pass. Facts: cell_override with old_value NULL per filled cell (bulk-summarized per
player-week to keep the facts table sane: one fact per (row, column)).

    python -m scripts.sota_recon.build_pbp_backfill_v26            # DRY RUN + licenses
    python -m scripts.sota_recon.build_pbp_backfill_v26 --apply
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
from .sources import PBP_MERGED, PBP_ROLLUP, latest_v26

WAVE = "wave47.pbp_backfill"
LICENSE_MIN = 0.95
FILL_LO, FILL_HI = 1978, 1998
VAL_LO, VAL_HI = 1999, 2015

# v26 col -> (pbp player-id col, aggregation SQL over plays, play filter, validation window)
# validation window = years where v26 already carries the column (charted cols are 2018+)
DERIVES = {
    "receiving_target_interceptions": (
        "receiver_player_id", "COUNT(*)", "interception = 1", (2018, 2024)),
    "sack_yards_lost": (
        "passer_player_id", "SUM(-yards_gained)", "sack = 1", (VAL_LO, VAL_HI)),
    "punt_return_long": (
        "punt_returner_player_id", "MAX(return_yards)", "play_type = 'punt'", (VAL_LO, VAL_HI)),
    "kickoff_return_long": (
        "kickoff_returner_player_id", "MAX(return_yards)", "play_type = 'kickoff'", (VAL_LO, VAL_HI)),
    "punt_long": (
        "punter_player_id", "MAX(kick_distance)", "play_type = 'punt'", (VAL_LO, VAL_HI)),
}


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build_fills(con, lo: int, hi: int) -> None:
    """TEMP TABLE fills(player_week, <derived cols>) for seasons lo..hi."""
    pbp, roll = _q(PBP_MERGED), _q(PBP_ROLLUP)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE idmap AS
        SELECT pbp_player_id, ANY_VALUE(NFL_player_id) AS NFL_player_id
        FROM '{roll}' WHERE pbp_player_id IS NOT NULL AND NFL_player_id IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id) = 1""")
    parts = []
    for col, (idcol, agg, filt, _vw) in DERIVES.items():
        parts.append(f"""
        SELECT m.NFL_player_id || '_' || p.season || '_' || p.week AS player_week,
               '{col}' AS col, {agg} AS val
        FROM '{pbp}' p JOIN idmap m ON m.pbp_player_id = p.{idcol}
        WHERE p.season BETWEEN {lo} AND {hi} AND {filt} AND p.{idcol} IS NOT NULL
        GROUP BY 1, 2""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fills AS
        PIVOT ({' UNION ALL '.join(parts)}) ON col USING ANY_VALUE(val)
        GROUP BY player_week""")
    fcols = {r[0] for r in con.execute("DESCRIBE fills").fetchall()}
    for col in DERIVES:
        if col not in fcols:
            con.execute(f"ALTER TABLE fills ADD COLUMN {col} DOUBLE")


def _license(con, vq: str) -> dict:
    """Validate each derivation on its own overlap window; only >=95% agreement may fill."""
    out = {}
    for col in DERIVES:
        vlo, vhi = DERIVES[col][3]
        _build_fills(con, vlo, vhi)
        n, agree = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(v.{col} - f.{col}) <= 0.5)
            FROM fills f JOIN '{vq}' v USING (player_week)
            WHERE v.{col} IS NOT NULL AND f.{col} IS NOT NULL""").fetchone()
        pct = (agree / n) if n else None
        out[col] = {"overlap_n": n, "agree_pct": None if pct is None else round(pct, 4),
                    "licensed": bool(pct and pct >= LICENSE_MIN)}
    return out


def run(apply: bool = False) -> dict:
    vq = _q(latest_v26())
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    lic = _license(con, vq)
    licensed = [c for c in DERIVES if lic[c]["licensed"]]

    _build_fills(con, FILL_LO, FILL_HI)
    plan = {}
    for col in licensed:
        fill_n, disagree_n = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE v.{col} IS NULL AND f.{col} IS NOT NULL),
                   COUNT(*) FILTER (WHERE v.{col} IS NOT NULL AND f.{col} IS NOT NULL
                                    AND ABS(v.{col} - f.{col}) > 0.5)
            FROM fills f JOIN '{vq}' v USING (player_week)""").fetchone()
        plan[col] = {"null_cells_to_fill": fill_n,
                     "existing_disagreements_reported_not_touched": disagree_n}
    diag = {"licenses": lic, "fill_plan": plan,
            "unlicensed": [c for c in DERIVES if c not in licensed]}
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}

    stamp = utc_stamp()
    vp = Path(latest_v26())
    tmp = vp.with_name(vp.stem + "_w47.parquet")
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    replaces = ", ".join(
        f"COALESCE(v.{c}, f.{c}) AS {c}" for c in licensed)
    r = con.execute(f"""
        SELECT v.* REPLACE ({replaces})
        FROM '{vq}' v LEFT JOIN fills f USING (player_week)
    """).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close()

    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    checks = {}
    ok = after == before
    for col in licensed:
        filled, lost = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE o.{col} IS NULL AND n.{col} IS NOT NULL),
                   COUNT(*) FILTER (WHERE o.{col} IS NOT NULL
                                    AND (n.{col} IS NULL OR n.{col} <> o.{col}))
            FROM '{vq}' o JOIN '{tq}' n USING (player_week)
            WHERE o.player_week IS NOT NULL""").fetchone()
        checks[col] = {"filled": filled, "existing_cells_changed": lost}
        ok = ok and lost == 0 and filled == plan[col]["null_cells_to_fill"]

    from . import golden_samples
    import scripts.sota_recon.sources as S
    orig = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = orig
    ok = ok and g["failed"] == 0

    res = {"mode": "APPLY", **diag, "before": before, "after": after,
           "apply_checks": checks, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(ok)}
    if ok:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        for col in licensed:
            rows = con.execute(f"""
                SELECT v.player_week, f.{col} FROM fills f JOIN '{vq}' v USING (player_week)
                WHERE v.{col} IS NULL AND f.{col} IS NOT NULL""").fetchall()
            for pw, val in rows:
                facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                                target_key=pw, column_name=col,
                                old_value=None, new_value=str(val),
                                wave_id=WAVE, reason="1978-98 NULL cell derived from merged PBP",
                                witness="pbp_merged_1978_2025",
                                source_snapshot_id=snap)
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew47_{stamp}.parquet")
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
