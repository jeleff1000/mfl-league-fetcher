"""STAGE 7: derived columns close by RECOMPUTE from their (closed) bases.

v3: driven by the LICENSED registry (stage7_formulas.v1.json -- every
formula empirically vouched >=99% on 2015-2024), not a hand list. Emits one
overlay car covering every licensed column: cells where the stored value is
NULL-with-bases or contradicts its own bases are repaired to the recompute.

ITERATIVE BY NATURE (measured 2026-08-03): base backfills create newly
recompute-eligible cells, so Stage 7 re-runs after each base batch until the
overlay comes back empty. Convergence is guaranteed once bases are locked.

Run: python scripts/sota_recon/stage7_recompute.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
REGISTRY = Path(__file__).parent / "witness_gate" / "contracts" / "stage7_formulas.v1.json"
OVERLAY = LAKE / "weekly_overlay_stage7.parquet"
RECEIPT = LAKE / "stage7_receipt.json"
# PRINT CONVENTION IS PER COLUMN AND MEASURED, never assumed (three
# tolerance fictions on 2026-08-03: 0.005 manufactured 41k rounding-residue
# repairs; 0.045 flagged exact half-unit cells (stored 143.8 = recompute
# 143.75 rounded); and unrounded writes then missed witnesses by float
# epsilon at their own 0.05 boundary). RULE: a column whose modern cells are
# all one-decimal STORES ROUNDED -- recompute writes ROUND(expr, 1) and
# repairs only cells that differ from that; a full-precision column writes
# the raw expr at 0.005.
TOL_FULL = 0.005


def print_scale_is_one_decimal(con, wk: str, col: str) -> bool:
    n, ok = con.execute(f"""
    SELECT COUNT(*), COUNT(*) FILTER (
      WHERE ABS(TRY_CAST({col} AS DOUBLE) * 10
                - ROUND(TRY_CAST({col} AS DOUBLE) * 10)) < 1e-6)
    FROM read_parquet('{wk}')
    WHERE season_type = 'REG' AND CAST(year AS INT) BETWEEN 2015 AND 2024
      AND {col} IS NOT NULL""").fetchone()
    return bool(n) and ok / n >= 0.999


def build(con: duckdb.DuckDBPyConnection) -> dict:
    reg = json.loads(REGISTRY.read_text(encoding="utf-8"))
    licensed = reg["licensed"]
    wk = Path(S.latest_v26()).as_posix()
    parts, per = [], {}
    for col, spec in licensed.items():
        expr, guard = spec["expr"], spec["guard"]
        if "OVER (" in expr:
            per[col] = "SKIPPED: window-function share, not row-local"
            continue
        # storage convention is DECLARED in the registry from lock evidence
        # (majority-vote detection failed on the mixed-convention plane and
        # the gate refused batch 7 at 0.096 -- never infer this again)
        one_dec = spec.get("storage") == "1dp"
        write = f"ROUND(({expr}), 1)" if one_dec else f"({expr})"
        tol = 0.001 if one_dec else TOL_FULL
        n, ok = con.execute(f"""
        SELECT COUNT(*),
          COUNT(*) FILTER (WHERE ABS(TRY_CAST({col} AS DOUBLE) - {write})
                           <= {tol})
        FROM read_parquet('{wk}')
        WHERE season_type = 'REG' AND ({guard}) > 0
          AND {col} IS NOT NULL""").fetchone()
        per[col] = {"cells_with_base": n, "recompute_exact": ok,
                    "agree": round(ok / n, 4) if n else None,
                    "print_scale": "1dp" if one_dec else "full"}
        parts.append(f"""
        SELECT NFL_player_id, year, week, '{col}' AS column_name,
               TRY_CAST({col} AS DOUBLE) AS old_value,
               {write} AS new_value, 'stage7' AS repair_id,
               'recompute' AS root,
               'DERIVED-RECOMPUTED from closed bases (Stage 7)' AS ruling
        FROM (SELECT *, COUNT(*) OVER (
                PARTITION BY NFL_player_id, year, week) AS __nrow
              FROM read_parquet('{wk}') WHERE season_type = 'REG')
        -- DH discipline, counted BEFORE any filter: a WHERE-then-QUALIFY
        -- count saw a lone mismatching DH row as "single" and the applier's
        -- week-keyed join then clobbered its sibling (59-cell oscillation,
        -- caught by the fixpoint check 2026-08-03). DH rows are the
        -- applier's row-local jurisdiction, never the overlay's.
        WHERE __nrow = 1 AND ({guard}) > 0
          AND ({write}) IS NOT NULL
          AND (({col} IS NULL)
               OR ABS(TRY_CAST({col} AS DOUBLE) - {write}) > {tol})""")
    union = " UNION ALL ".join(parts)
    con.execute(f"COPY ({union}) TO '{OVERLAY.as_posix()}' (FORMAT parquet)")
    n_ov = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OVERLAY.as_posix()}')"
    ).fetchone()[0]
    by_col = dict(con.execute(f"""
        SELECT column_name, COUNT(*)
        FROM read_parquet('{OVERLAY.as_posix()}') GROUP BY 1""").fetchall())
    receipt = {"wave": "stage7_licensed_round", "date": time.strftime("%Y-%m-%d"),
               "registry": str(REGISTRY), "columns_licensed": len(licensed),
               "per_column": per, "repair_cells": n_ov,
               "repair_cells_by_column": {k: int(v) for k, v in by_col.items()},
               "overlay": str(OVERLAY),
               "law": ("bases are truth; iterative until the overlay comes "
                       "back empty")}
    RECEIPT.write_text(json.dumps(receipt, indent=1, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='1200MB'")
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    r = build(con)
    print(json.dumps({"repair_cells": r["repair_cells"],
                      "by_column": r["repair_cells_by_column"]}, indent=1))
