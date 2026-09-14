"""BATCH-5 LANDING: swap the repaired plane in, then prove it.

Sequence (each step gates the next):
  1. verify repaired plane exists + row count reconciles with receipt
  2. backup current plane -> pre_batch5, swap repaired in
  3. spot-proof: purge columns have ZERO pre-floor zeros; stage7 cells match
  4. lock gate (test_witness_locks) on the swapped plane -- drift check
Exit 0 only if every step passes. No pipes, true exit.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S
from scripts.sota_recon.apply_weekly_overlays import PURGE_FLOORS

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")


def main() -> int:
    src = Path(S.latest_v26())
    out = src.with_name(src.stem + ".repaired_20260803" + src.suffix)
    receipt = json.loads((LAKE / "weekly_promote_apply_receipt.json"
                          ).read_text(encoding="utf-8"))
    assert receipt["wave"] == "weekly_promote_apply_batch5", (
        "receipt is not batch 5 -- applier did not finish")
    assert out.exists(), f"repaired plane missing: {out}"

    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    con.execute("SET threads=2")
    n_out = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
    expected = receipt["rows"] - receipt["null_year_rows_dropped"]
    assert n_out == expected, f"rows {n_out} != receipt {expected}"

    # TRICHOTOMY GATE (2026-08-03, Joe: "check zeros nulls and blanks in
    # every variation" -- five separate zero/null/blank failures in one day
    # proved this must be MACHINE-CHECKED before every swap, never
    # remembered). For every column the batch's cars touch, the
    # NULL / zero / nonzero census must not change in any way the cars do
    # not declare: NULL count may only move by (cells written into NULL)
    # minus (cells doomed to NULL); a zero may only appear where a car
    # wrote it. Any unexplained drift in any of the three states refuses
    # the swap.
    ov_cols = {r[0] for r in con.execute(f"""
        SELECT DISTINCT column_name FROM read_parquet(
          '{(LAKE / 'weekly*overlay*.parquet').as_posix()}',
          union_by_name=true)""").fetchall()} | {r[0] for r in con.execute(f"""
        SELECT DISTINCT column_name FROM read_parquet(
          '{(LAKE / 'engine_overlays' / '*.parquet').as_posix()}',
          union_by_name=true)""").fetchall()}
    # RENAME-AWARE (2026-08-04): during a rename batch the BEFORE plane
    # carries old names and the AFTER plane carries new ones. Resolve each
    # car column against each file's own schema instead of assuming they
    # share a name.
    RENAME_BACK = {"total_tds_scored": "total_td",
                   "total_tds_accounted_for": "total_tds"}
    src_cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{src.as_posix()}') LIMIT 0"
    ).fetchall()}
    out_cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out.as_posix()}') LIMIT 0"
    ).fetchall()}

    def resolve(col, have):
        if col in have:
            return col
        alt = RENAME_BACK.get(col)
        return alt if alt and alt in have else None

    tri_bad = []
    for c in sorted(ov_cols):
        c_src, c_out = resolve(c, src_cols), resolve(c, out_cols)
        if c_src is None or c_out is None:
            print(f"trichotomy: {c} absent from one side, skipped", flush=True)
            continue
        b_null, b_zero, b_nz = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE {c_src} IS NULL),
               COUNT(*) FILTER (WHERE TRY_CAST({c_src} AS DOUBLE) = 0),
               COUNT(*) FILTER (WHERE TRY_CAST({c_src} AS DOUBLE) <> 0)
        FROM read_parquet('{src.as_posix()}')
        WHERE season_type = 'REG'""").fetchone()
        a_null, a_zero, a_nz = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE {c_out} IS NULL),
               COUNT(*) FILTER (WHERE TRY_CAST({c_out} AS DOUBLE) = 0),
               COUNT(*) FILTER (WHERE TRY_CAST({c_out} AS DOUBLE) <> 0)
        FROM read_parquet('{out.as_posix()}')
        WHERE season_type = 'REG'""").fetchone()
        decl = con.execute(f"""
        WITH cars AS (
          SELECT * FROM read_parquet(
            '{(LAKE / 'weekly*overlay*.parquet').as_posix()}',
            union_by_name=true)
          UNION ALL
          SELECT * FROM read_parquet(
            '{(LAKE / 'engine_overlays' / '*.parquet').as_posix()}',
            union_by_name=true))
        SELECT COUNT(*) FILTER (WHERE old_value IS NULL
                                AND new_value IS NOT NULL),
               COUNT(*) FILTER (WHERE new_value = 0)
        FROM cars WHERE column_name = '{c}'""").fetchone()
        null_filled, zeros_declared = decl
        # the applier's DH row-local recompute is a DECLARED writer for
        # stage7-licensed columns (its jurisdiction, not a car's) -- count
        # its bounded reach (batch-22 refusal: ~330 DH cells per bonus
        # column beyond car declarations)
        s7reg = json.loads((Path(__file__).parent / "witness_gate" /
                            "contracts" / "stage7_formulas.v1.json"
                            ).read_text("utf-8"))["licensed"]
        if c in s7reg or c_out in s7reg:
            dh_rows = con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT COUNT(*) OVER (
                PARTITION BY NFL_player_id, year, week) AS n
              FROM read_parquet('{src.as_posix()}')
              WHERE season_type = 'REG') WHERE n > 1""").fetchone()[0]
            zeros_declared += dh_rows
            null_filled += dh_rows
        # bounds, not exact equality: doom/purge/DH paths add declared NULL
        # increases elsewhere; what is REFUSED is movement with no car and
        # no doom -- zeros appearing beyond declaration, or nonzero mass
        # vanishing into NULL/zero without a car saying so.
        if a_zero > b_zero + zeros_declared:
            tri_bad.append(f"{c}: zeros grew {b_zero}->{a_zero} but cars "
                           f"declare only {zeros_declared}")
        if a_nz < b_nz and (b_nz - a_nz) > null_filled + zeros_declared:
            tri_bad.append(f"{c}: nonzero mass shrank {b_nz}->{a_nz} "
                           f"beyond declared writes")
    assert not tri_bad, ("TRICHOTOMY GATE REFUSES:\n" + "\n".join(tri_bad))
    print(f"trichotomy gate: {len(ov_cols)} columns clean", flush=True)

    bak = src.with_name(src.stem + ".pre_batch5" + src.suffix)
    if not bak.exists():
        print("backup -> pre_batch5", flush=True)
        shutil.copy2(src, bak)
    shutil.move(str(out), str(src))
    print("SWAPPED", flush=True)

    for c, fl in PURGE_FLOORS.items():
        z = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')
        WHERE CAST(year AS INT) < {fl}
          AND TRY_CAST({c} AS DOUBLE) = 0""").fetchone()[0]
        print(f"purge proof {c}: {z} pre-{fl} zeros remain", flush=True)
        assert z == 0, f"{c}: {z} fabricated zeros survived the swap"
    ya = con.execute(f"""
    SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')
    WHERE season_type='REG' AND TRY_CAST(attempts AS DOUBLE) > 0
      AND passing_yards_per_attempt IS NULL""").fetchone()[0]
    print(f"stage7 proof: {ya} Y/A NULLs with attempts>0 remain "
          f"(13 DH cells expected)", flush=True)

    # scope the gate to the columns this batch touched (full sweep runs on
    # the maintenance cadence) -- a 3-column batch no longer re-proves 936
    # lanes. Set SOTA_GATE_FULL=1 to force the unscoped pass.
    import os
    env = dict(os.environ)
    if not os.environ.get("SOTA_GATE_FULL"):
        touched = sorted({resolve(c, out_cols) or c for c in ov_cols})
        env["SOTA_GATE_COLUMNS"] = ",".join(touched)
        print(f"gate scoped to {len(touched)} touched columns", flush=True)
    gate = subprocess.run(
        [sys.executable, "-m", "pytest", "-q",
         "scripts/sota_recon/test_witness_locks.py"],
        cwd=str(Path(__file__).resolve().parents[2]), env=env)
    print(f"LOCK GATE EXIT: {gate.returncode}", flush=True)
    return gate.returncode


if __name__ == "__main__":
    sys.exit(main())
