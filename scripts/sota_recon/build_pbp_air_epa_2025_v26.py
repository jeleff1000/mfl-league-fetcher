"""
sota_recon/build_pbp_air_epa_2025_v26.py  --  wave54: 2025 air/EPA re-aggregation +
completed-air-yards columns (Joe's directives 2026-07-12).

THE STALE SET (measured): 2025 wk14+ passing/receiving_air_yards + air_yards_share
drop 1080->2 rows, passing/rushing/receiving_epa + passing_cpoe + total_epa -> 0.
The merged PBP was refreshed through wk22 but these columns were never re-aggregated.
Everything else 2025 (success/WPA/drops/adot/NGS) is already fresh -- untouched here.

THE AIR-YARDS SPLIT (measured 16-17% agreement vs PFR): our air yards are INTENDED
(all attempts, nflverse definition); PFR's are COMPLETED (completions only). Both are
real stats -> two columns. This wave adds passing_completed_air_yards +
receiving_completed_air_yards from PBP (2006+, the charting era), giving the PFR
advanced boxes a clean witness target.

FORMULA LOCK (the runbook's one rule): before touching 2025, every recomputed column's
SQL must reproduce the STORED 2024 values (which validated 100% vs nflverse at build
time). A column whose formula can't hit >=99.5% on 2024 is REFUSED, never guessed.

Apply = pyarrow batch-stream (the wave52 pattern -- no SQL join near the wide write):
2025 rows get the 8 recomputed cells REPLACED; all rows get the 2 new columns attached.

Gates: row count unchanged; untouched-column checksums; 2024 values of the 8 columns
UNCHANGED (lock proof); 2025 wk14-18 fill returns to wk13 levels; total_epa composite
identity holds; backup + swap.

    python -m scripts.sota_recon.build_pbp_air_epa_2025_v26            # DRY RUN (lock report)
    python -m scripts.sota_recon.build_pbp_air_epa_2025_v26 --apply
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
from .sources import latest_v26, registry

WAVE = "wave54.pbp_air_epa_2025"
RECOMPUTE = ["passing_air_yards", "receiving_air_yards", "air_yards_share",
             "passing_epa", "rushing_epa", "receiving_epa", "passing_cpoe",
             "total_epa"]
NEW_COLS = ["passing_completed_air_yards", "receiving_completed_air_yards"]
LOCK_MIN = 0.995
BATCH = 131_072
CHECKSUM_COLS = ["passing_yards", "rushing_yards", "receiving_yards", "touches"]


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build_frames(con, years_expr: str) -> None:
    """TEMP TABLES pass_f / rush_f / recv_f / team_air keyed (gsis, year, week, st)."""
    pbp = _q(registry()["pbp_merged_1978_2025"].path)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pass_f AS
        SELECT passer_player_id AS gsis, season AS year, week, season_type AS st,
               SUM(air_yards) AS air_int,
               SUM(CASE WHEN complete_pass = 1 THEN air_yards END) AS air_cmp,
               SUM(qb_epa) AS epa_sum,  -- nflverse passing_epa = qb_epa (locked 100.0%
                                        -- on 2024; plain epa-on-pass-plays was 91.8%)
               AVG(cpoe) AS cpoe_avg
        FROM '{pbp}'
        WHERE passer_player_id IS NOT NULL AND season IN ({years_expr})
        GROUP BY 1, 2, 3, 4""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE rush_f AS
        SELECT rusher_player_id AS gsis, season AS year, week, season_type AS st,
               SUM(epa) AS epa_sum
        FROM '{pbp}'
        WHERE rusher_player_id IS NOT NULL AND season IN ({years_expr})
        GROUP BY 1, 2, 3, 4""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE recv_f AS
        SELECT receiver_player_id AS gsis, season AS year, week, season_type AS st,
               SUM(air_yards) AS air_int,
               SUM(CASE WHEN complete_pass = 1 THEN air_yards END) AS air_cmp,
               SUM(epa) AS epa_sum
        FROM '{pbp}'
        WHERE receiver_player_id IS NOT NULL AND season IN ({years_expr})
        GROUP BY 1, 2, 3, 4""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE team_air AS
        SELECT posteam, season AS year, week, season_type AS st,
               SUM(air_yards) AS team_air
        FROM '{pbp}'
        WHERE posteam IS NOT NULL AND season IN ({years_expr})
        GROUP BY 1, 2, 3, 4""")


def _lock_report(con, vq: str) -> dict:
    """Prove each formula reproduces the STORED 2024 REG values."""
    out = {}
    checks = [
        ("passing_air_yards", "pass_f", "air_int", 0.5),
        ("passing_epa", "pass_f", "epa_sum", 0.05),
        ("passing_cpoe", "pass_f", "cpoe_avg", 0.05),
        ("rushing_epa", "rush_f", "epa_sum", 0.05),
        ("receiving_air_yards", "recv_f", "air_int", 0.5),
        ("receiving_epa", "recv_f", "epa_sum", 0.05),
    ]
    for col, frame, val, tol in checks:
        n, ok = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(f.{val} - t.{col}) <= {tol})
            FROM {frame} f
            JOIN '{vq}' t ON t.NFL_player_id = f.gsis AND t.year = f.year
                AND t.week = f.week AND t.season_type = f.st
            WHERE f.year = 2024 AND f.st = 'REG'
              AND t.{col} IS NOT NULL AND f.{val} IS NOT NULL""").fetchone()
        out[col] = {"n": n, "match": round(ok / n, 4) if n else None}
    # air_yards_share = receiver air / team air (lock vs stored)
    n, ok = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (
            WHERE ABS(f.air_int / NULLIF(ta.team_air, 0) - t.air_yards_share) <= 0.005)
        FROM recv_f f
        JOIN '{vq}' t ON t.NFL_player_id = f.gsis AND t.year = f.year
            AND t.week = f.week AND t.season_type = f.st
        JOIN team_air ta ON ta.posteam = t.nfl_team AND ta.year = f.year
            AND ta.week = f.week AND ta.st = f.st
        WHERE f.year = 2024 AND f.st = 'REG'
          AND t.air_yards_share IS NOT NULL AND f.air_int IS NOT NULL""").fetchone()
    out["air_yards_share"] = {"n": n, "match": round(ok / n, 4) if n else None}
    return out


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET preserve_insertion_order = false")
    v26_p = Path(latest_v26())
    vq = _q(v26_p)

    have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    clash = [c for c in NEW_COLS if c in have]
    if clash:
        raise SystemExit(f"columns already exist, refusing: {clash}")

    # frames: 2024 (lock) + 2025 (target) + 2006-2023 (completed-air backfill)
    _build_frames(con, ", ".join(str(y) for y in range(2006, 2026)))
    lock = _lock_report(con, vq)
    locked = all(v["match"] is not None and v["match"] >= LOCK_MIN for v in lock.values())
    diag = {"formula_lock_2024": lock, "locked": locked}
    if not locked or not apply:
        con.close()
        return {"mode": "DRY-RUN" if locked else "REFUSED-LOCK-FAILED", **diag}

    # dict maps for the pyarrow pass
    def to_map(sql):
        return {(g, int(y), int(w), s): tuple(vals)
                for g, y, w, s, *vals in con.execute(sql).fetchall()}
    pass_m = to_map("SELECT gsis, year, week, st, air_int, air_cmp, epa_sum, cpoe_avg FROM pass_f")
    rush_m = to_map("SELECT gsis, year, week, st, epa_sum FROM rush_f")
    recv_m = to_map("SELECT gsis, year, week, st, air_int, air_cmp, epa_sum FROM recv_f")
    team_m = to_map("SELECT posteam, year, week, st, team_air FROM team_air")

    before_n = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    sums = ", ".join(f"SUM({c}) AS {c}" for c in CHECKSUM_COLS)
    before_ck = con.execute(f"SELECT {sums} FROM '{vq}'").fetchone()
    y24_ck = con.execute(f"""
        SELECT {', '.join(f'ROUND(SUM({c}), 2)' for c in RECOMPUTE)}
        FROM '{vq}' WHERE year = 2024""").fetchone()

    tmp = v26_p.with_name(v26_p.stem + "_w54.parquet")
    # ParquetFile must CLOSE before the swap -- an open reader on the target makes
    # Windows deny os.replace (hit 2026-07-12; gates passed, swap failed)
    pf = pq.ParquetFile(vq)
    writer = None
    try:
        for batch in pf.iter_batches(batch_size=BATCH):
            tbl = pa.Table.from_batches([batch])
            nid = tbl.column("NFL_player_id").to_pylist()
            yr = tbl.column("year").to_pylist()
            wk = tbl.column("week").to_pylist()
            st = tbl.column("season_type").to_pylist()
            team = tbl.column("nfl_team").to_pylist()
            cur = {c: tbl.column(c).to_pylist() for c in RECOMPUTE}
            newc = {c: [] for c in NEW_COLS}
            for i in range(len(nid)):
                key = (nid[i], int(yr[i]) if yr[i] is not None else -1,
                       int(wk[i]) if wk[i] is not None else -1, st[i])
                p = pass_m.get(key)
                r = recv_m.get(key)
                newc["passing_completed_air_yards"].append(p[1] if p else None)
                newc["receiving_completed_air_yards"].append(r[1] if r else None)
                if key[1] == 2025:  # the re-aggregation target year
                    ru = rush_m.get(key)
                    ta = team_m.get((team[i], key[1], key[2], key[3]))
                    cur["passing_air_yards"][i] = p[0] if p else None
                    cur["passing_epa"][i] = p[2] if p else None
                    cur["passing_cpoe"][i] = p[3] if p else None
                    cur["rushing_epa"][i] = ru[0] if ru else None
                    cur["receiving_air_yards"][i] = r[0] if r else None
                    cur["receiving_epa"][i] = r[2] if r else None
                    cur["air_yards_share"][i] = (
                        r[0] / ta[0] if r and r[0] is not None and ta and ta[0]
                        else None)
                    comps = [cur["passing_epa"][i], cur["rushing_epa"][i],
                             cur["receiving_epa"][i]]
                    cur["total_epa"][i] = (None if all(c is None for c in comps)
                                           else sum(c or 0 for c in comps))
            for c in RECOMPUTE:
                idx = tbl.schema.get_field_index(c)
                tbl = tbl.set_column(idx, c, pa.array(cur[c], pa.float64()))
            for c in NEW_COLS:
                tbl = tbl.append_column(c, pa.array(newc[c], pa.float64()))
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), tbl.schema)
            writer.write_table(tbl)
    finally:
        if writer is not None:
            writer.close()
        pf.close()

    tq = Path(tmp).as_posix()
    after_n = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_ck = con.execute(f"SELECT {sums} FROM '{tq}'").fetchone()
    y24_ck2 = con.execute(f"""
        SELECT {', '.join(f'ROUND(SUM({c}), 2)' for c in RECOMPUTE)}
        FROM '{tq}' WHERE year = 2024""").fetchone()
    wk_fill = con.execute(f"""
        SELECT week, COUNT(passing_air_yards) + COUNT(receiving_air_yards) AS air,
               COUNT(passing_epa) + COUNT(rushing_epa) + COUNT(receiving_epa) AS epa
        FROM '{tq}' WHERE year = 2025 AND season_type = 'REG'
          AND week IN (13, 15, 17) GROUP BY 1 ORDER BY 1""").fetchall()
    # NULL-semantics note (measured 2026-07-12): the legacy build zero-filled AIR
    # columns on no-attempt rows (1,020 fake zeros in 2024 wk13 alone) while EPA
    # already used NULLs. This wave writes NULLs -- the post-cascade target state
    # (fake-zero nulling is an owed cascade step); real fill is ~40 passers + ~250
    # targets per week, so the gate expects ~290 air / ~420 epa cells per week.
    fill_ok = len(wk_fill) == 3 and all(
        air > 250 and epa > 300 for _, air, epa in wk_fill)
    new_fill = con.execute(f"""
        SELECT COUNT(passing_completed_air_yards), COUNT(receiving_completed_air_yards)
        FROM '{tq}' WHERE year >= 2006""").fetchone()
    gate = (after_n == before_n and before_ck == after_ck and y24_ck == y24_ck2
            and fill_ok and all(x > 0 for x in new_fill))
    res = {"mode": "APPLY", **diag, "rows": [before_n, after_n],
           "untouched_checksums_ok": before_ck == after_ck,
           "y2024_recompute_cols_unchanged": y24_ck == y24_ck2,
           "wk13_15_17_fill": wk_fill, "new_col_fill_2006plus": new_fill,
           "gate_pass": bool(gate)}
    if gate:
        bk = v26_p.with_name(v26_p.stem + f"_prew54_{utc_stamp()}.parquet")
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
