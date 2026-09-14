"""COEFFICIENT LICENSING for internal-metric pts_* components.

For each disposed internal-metric column, propose base columns by token
match, discover the coefficient as the MEDIAN ratio on modern nonzero
cells, and license `k * base` only if it reproduces >= 99.9% of cells at
0.011 tolerance. Single-base fits only -- multi-base composites (pass
components with yards+TD+INT terms) are left for formula specification.
Licensed entries land in the stage7 registry; the recompute batch closes
them.
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
REG = Path(__file__).parent / "witness_gate" / "contracts" / "stage7_formulas.v1.json"
ER = Path(__file__).parent / "witness_gate" / "contracts" / "era_rulings.v1.json"
TOK = re.compile(r"[a-z]+")
STOP = {"pts", "idp", "n", "the", "of"}


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{wk}') LIMIT 0").fetchall()]
    colset = set(cols)
    con.execute(f"""CREATE OR REPLACE TEMP VIEW modern AS
    SELECT * FROM read_parquet('{wk}')
    WHERE season_type = 'REG' AND CAST(year AS INT) BETWEEN 2015 AND 2024""")
    disp = json.loads(ER.read_text("utf-8")).get("dispositions", {})
    reg = json.loads(REG.read_text("utf-8"))
    targets = [c for c in disp
               if c.startswith("pts_") and c not in reg["licensed"]]
    print(f"{len(targets)} pts_* targets", flush=True)
    licensed, multi = 0, []
    for c in targets:
        want = set(TOK.findall(c.lower())) - STOP
        cands = sorted(
            (b for b in colset
             if b != c and not b.startswith(("pts_", "fpts_", "rank_"))
             and want & (set(TOK.findall(b.lower())) - STOP)),
            key=lambda b: -len(want & set(TOK.findall(b.lower()))))[:4]
        best = None
        for b in cands:
            try:
                k = con.execute(f"""
                SELECT MEDIAN(TRY_CAST({c} AS DOUBLE)
                              / TRY_CAST({b} AS DOUBLE))
                FROM modern
                WHERE TRY_CAST({b} AS DOUBLE) > 0
                  AND {c} IS NOT NULL""").fetchone()[0]
            except Exception:
                continue
            if k is None or abs(k) < 1e-9:
                # ZERO-COEFFICIENT BAN (2026-08-04): k=0 means the modern
                # window is all-zero and "0*anything" reproduces it -- the
                # fabricated-zero trap wearing a license. A coefficient
                # must be a NONZERO observed ratio (negatives are real: penalty points) or there is no formula.
                continue
            k = round(k, 4)
            try:
                n, ok = con.execute(f"""
                SELECT COUNT(*), COUNT(*) FILTER (
                  WHERE ABS(TRY_CAST({c} AS DOUBLE)
                            - {k} * COALESCE(TRY_CAST({b} AS DOUBLE), 0))
                        <= 0.011)
                FROM modern WHERE {c} IS NOT NULL""").fetchone()
            except Exception:
                continue
            agree = ok / n if n else 0.0
            if n >= 10000 and agree >= 0.999 and (
                    best is None or agree > best[2]):
                best = (b, k, agree, n)
        if best:
            b, k, agree, n = best
            reg["licensed"][c] = {
                "variant": f"coefficient {k}*{b}",
                "agree": round(agree, 5),
                "expr": f"{k} * COALESCE(TRY_CAST({b} AS DOUBLE), 0)",
                "guard": "1", "storage": None,
                "license_basis": (f"coefficient discovered by median ratio "
                                  f"on modern cells; reproduces {agree:.4f} "
                                  f"of n={n}")}
            licensed += 1
            print(f"  LICENSED {c} = {k} * {b} ({agree:.5f})", flush=True)
        else:
            multi.append(c)
    REG.write_text(json.dumps(reg, indent=1), encoding="utf-8")
    (LAKE / "coefficient_multibase_queue.json").write_text(
        json.dumps({"multi_base_or_unfit": multi}, indent=1),
        encoding="utf-8")
    print(f"DONE: {licensed} licensed, {len(multi)} multi-base/unfit queued",
          flush=True)


if __name__ == "__main__":
    main()
