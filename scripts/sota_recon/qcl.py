"""
sota_recon/qcl.py  --  QUALITY_COVERAGE_LEDGER generator, skeleton (WS8a / Phase 0B).

The QCL is GENERATED, never hand-edited: one row per (table, column), rebuilt
deterministically from the static registry inputs. Hand edits are defects -- regeneration
overwrites them. Run evidence (which run verified what) lives in append-only run
manifests, NOT here; "last verified" is a join, not a column write.

Inputs (grow as the registries land):
  * table schemas (weekly / season / career parquets of the current release)
  * aggregation types from recon_rate_fingerprint.REGISTRY
  * hard bound contracts from recon_bounds.HARD_BOUNDS
  * is_derived heuristic (pts_/rank_/lamar/fpts_/ppg_/bonus_/percentile prefixes)
  * witnesses[] from the LICENSED witness map (MAPPING_LICENSES.json -- Phase 2)
  * precedence_rule from PRECEDENCE_REGISTRY.json (WS4b gate -- Phase 2)
Era-grained fill status arrives in Phase 3.

    python -m scripts.sota_recon.qcl        # regenerate QCL.parquet (idempotent)
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

from . import recon_bounds, recon_rate_fingerprint
from .sources import latest_v26

QCL_OUT = os.environ.get(
    "SOTA_QCL_PATH",
    r"D:\league-history-data\nfl\derived\validation\sota_recon_master\QCL.parquet",
)

DERIVED_PREFIXES = ("pts_", "rank_", "lamar", "fpts_", "ppg_", "bonus_", "percentile_",
                    "avg_pts_next_year")


def _tables() -> dict[str, str]:
    wk = latest_v26()
    sc = os.path.join(os.path.dirname(wk), "season_career_v26")
    return {
        "nfl_player_stats_all": wk,
        "player_nfl_season": os.path.join(sc, "player_nfl_season.parquet"),
        "player_nfl_season_all": os.path.join(sc, "player_nfl_season_all.parquet"),
        "player_nfl_career": os.path.join(sc, "player_nfl_career.parquet"),
        "player_nfl_career_all": os.path.join(sc, "player_nfl_career_all.parquet"),
    }


def _licensed_witnesses() -> dict[str, str]:
    """v26 column -> comma list of LICENSED witness sources (witness_map licenses).
    Fail-soft: no licenses file (fresh clone) leaves the column None."""
    try:
        from .witness_map import LICENSES
        import json
        rows = json.load(open(LICENSES, encoding="utf-8"))["rows"]
    except (FileNotFoundError, KeyError):
        return {}
    by_col: dict[str, list[str]] = {}
    for r in rows:
        if r["verdict"] == "VALIDATED":
            by_col.setdefault(r["v26_col"], []).append(r["source"])
    return {c: ",".join(sorted(s)) for c, s in by_col.items()}


def _precedence_rules() -> dict[str, str]:
    """v26 column -> compact per-era ruling string from PRECEDENCE_REGISTRY.json."""
    try:
        from .precedence import REGISTRY_OUT
        import json
        rows = json.load(open(REGISTRY_OUT, encoding="utf-8"))["rows"]
    except (FileNotFoundError, KeyError):
        return {}
    by_col: dict[str, list[str]] = {}
    for r in rows:
        if not r.get("ruling"):
            continue
        w = f"={r['winner']}" if r.get("winner") else ""
        by_col.setdefault(r["stat"], []).append(f"{r['era']}:{r['ruling']}{w}")
    return {c: " | ".join(v) for c, v in by_col.items()}


def generate(out: str | None = None) -> dict:
    out = out or QCL_OUT
    con = duckdb.connect()
    bounds_by_col: dict[str, list[str]] = {}
    for name, child, parent in recon_bounds.HARD_BOUNDS:
        bounds_by_col.setdefault(child, []).append(name)
        bounds_by_col.setdefault(parent, []).append(name)
    agg = {c: spec[0] for c, spec in recon_rate_fingerprint.REGISTRY.items()}
    witnesses = _licensed_witnesses()
    precedence = _precedence_rules()

    rows = []
    for table, path in _tables().items():
        cols = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM '{Path(path).as_posix()}' LIMIT 0").fetchall()]
        grain = "player_week" if table == "nfl_player_stats_all" else (
            "player_season" if "season" in table else "player_career")
        for c in cols:
            rows.append({
                "table_name": table,
                "column_name": c,
                "grain": grain,
                "is_derived": c.startswith(DERIVED_PREFIXES),
                "aggregation_type": agg.get(c) if grain != "player_week" else None,
                "contracts": ",".join(bounds_by_col.get(c, [])) or None,
                "witnesses": witnesses.get(c),
                "precedence_rule": precedence.get(c),
                "fill_status": None,      # Phase 3 (census join)
            })

    os.makedirs(os.path.dirname(out), exist_ok=True)
    import pyarrow as pa, pyarrow.parquet as pq
    tbl = pa.Table.from_pylist(rows)
    pq.write_table(tbl, out)
    con.close()

    n_typed = sum(1 for r in rows if r["aggregation_type"])
    n_contracted = sum(1 for r in rows if r["contracts"])
    return {"rows": len(rows), "with_aggregation_type": n_typed,
            "with_contracts": n_contracted, "out": out}


if __name__ == "__main__":
    r = generate()
    print(f"QCL: {r['rows']} column rows -> {r['out']}")
    print(f"  aggregation-typed: {r['with_aggregation_type']}  "
          f"contract-covered: {r['with_contracts']}")
    print("  (witnesses/precedence/fill_status columns present, populated in Phase 2/3)")
