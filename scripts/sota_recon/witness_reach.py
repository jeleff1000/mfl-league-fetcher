"""Measured per-column DENSITY of every witness source: when coverage actually starts.

Three generations of this measurement, each killed by a real defect (Joe, 2026-08-01):

1. Declared registry spans -- source facts, not column facts: award pages spanning
   1920 handed PFR a 1920 passing_yards reach while PFR's passing pages start 1932.
2. First NONZERO value -- one stray row opens the window, and recovery surfaces carry
   values injected from SUPER-TABLE lineage that the named source never published, so
   "PFR" inherited reach it does not have.
3. This version: a per-season DENSITY curve per (source, table, column) -- the share
   of that table's rows carrying a real nonzero value each season -- and a START
   defined as the first SUSTAINED qualifying season.

Qualifying season: >= 3 nonzero rows, or >= 1 when the table has under 30 rows that
season (award pages).  Percent thresholds alone were rejected: rare-event columns
(pick-sixes) are legitimately sparse even when fully tracked.  Sustained: another
qualifying season within the next two -- a lone 1921 curiosity never opens a window.
Padded zeros never qualify anywhere (the COALESCE-0 trap as a reach claim).

The receipt keeps: start (sustained), first_any (old min-nonzero, for comparison),
last, and the decade density scale {decade: pct} so the ledger can show when each
source actually turns on rather than when it first whispers.

    python -m scripts.sota_recon.witness_reach

Output: witness_reach.json beside the other receipts.
"""
from __future__ import annotations

import json
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from . import sources as S
from .witness_map import WITNESS_MAP

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
REACH = OUT / "witness_reach.json"

YEAR_CANDIDATES = ("year", "season", "year_id", "_rg_season")


def start_from_density(seasons: dict[int, tuple[int, int]]) -> int | None:
    """First sustained qualifying season from {year: (nonzero_rows, table_rows)}."""
    def qualifies(y: int) -> bool:
        nz, n = seasons[y]
        return nz >= 3 or (n < 30 and nz >= 1)

    years = sorted(y for y in seasons if qualifies(y))
    if not years:
        return None
    span_end = max(seasons)
    for i, y in enumerate(years):
        sustained = (i + 1 < len(years) and years[i + 1] - y <= 2)
        no_room = y >= span_end - 2   # nothing after it to sustain with
        if sustained or no_room:
            return y
    return None


def _measure_source(item: tuple[str, list]) -> list[dict]:
    """One scan per physical path: per-season nonzero density per (table, column)."""
    import duckdb

    source, specs = item
    src = S.registry()[source]
    out: list[dict] = []
    by_path: dict[str, list] = defaultdict(list)
    for spec in specs:
        by_path[spec.source_path or src.path].append(spec)
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    try:
        for path, path_specs in by_path.items():
            p = Path(path).as_posix()
            try:
                cols = {r[0] for r in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{p}', union_by_name=true)"
                ).fetchall()}
            except Exception as exc:
                out.append({"source": source, "path": path, "error": repr(exc)})
                continue
            year_col = next((c for c in YEAR_CANDIDATES if c in cols), None)
            if year_col is None:
                for spec in path_specs:
                    out.append({"source": source, "source_table": spec.source_table or "",
                                "source_col": spec.source_col, "start": None,
                                "why": "no year axis"})
                continue
            keys, exprs = [], []
            seen = set()
            for spec in path_specs:
                key = (spec.source_table or "", spec.source_col)
                if key in seen:
                    continue
                seen.add(key)
                if spec.source_col not in cols:
                    out.append({"source": source, "source_table": key[0],
                                "source_col": key[1], "start": None,
                                "why": "column absent"})
                    continue
                tbl_pred = "TRUE"
                if spec.table_col and spec.source_table and spec.table_col in cols:
                    tbl = spec.source_table.replace("'", "''")
                    tbl_pred = f'r."{spec.table_col}" = \'{tbl}\''
                # a padded zero is not presence; text needs non-blank
                val_pred = (f'{tbl_pred} AND r."{spec.source_col}" IS NOT NULL '
                            f'AND TRIM(CAST(r."{spec.source_col}" AS VARCHAR)) <> \'\' '
                            f'AND (TRY_CAST(r."{spec.source_col}" AS DOUBLE) IS NULL '
                            f'OR TRY_CAST(r."{spec.source_col}" AS DOUBLE) <> 0)')
                i = len(keys)
                keys.append(key)
                exprs.append(f'COUNT(*) FILTER (WHERE {val_pred}) AS nz_{i}, '
                             f'COUNT(*) FILTER (WHERE {tbl_pred}) AS n_{i}')
            if not keys:
                continue
            try:
                rows = con.execute(
                    f'SELECT TRY_CAST(r."{year_col}" AS INT) AS y, {", ".join(exprs)} '
                    f"FROM read_parquet('{p}', union_by_name=true) r "
                    f"WHERE TRY_CAST(r.\"{year_col}\" AS INT) IS NOT NULL "
                    f"GROUP BY 1").fetchall()
            except Exception as exc:
                out.append({"source": source, "path": path, "error": repr(exc)})
                continue
            for i, (table, col) in enumerate(keys):
                seasons = {int(r[0]): (int(r[1 + 2 * i]), int(r[2 + 2 * i]))
                           for r in rows if r[2 + 2 * i]}
                # A witness licence covers the source's REGISTERED span. Recovery
                # surfaces carry super-table-injected rows far outside it
                # (ancient_pbp1978_recovery, registered 1978-79, holds passing
                # yards 'from 1921'); per the lineage contract those rows are
                # their own finding, never reach for the named source.
                in_span = {y: v for y, v in seasons.items()
                           if src.year_min <= y <= src.year_max}
                oos_nz = sorted(y for y, (nz, _) in seasons.items()
                                if nz > 0 and not src.year_min <= y <= src.year_max)
                nz_years = sorted(y for y, (nz, _) in in_span.items() if nz > 0)
                decade: dict[int, list[int]] = defaultdict(lambda: [0, 0])
                for y, (nz, n) in in_span.items():
                    d = (y // 10) * 10
                    decade[d][0] += nz
                    decade[d][1] += n
                out.append({
                    "source": source, "source_table": table, "source_col": col,
                    "start": start_from_density(in_span) if in_span else None,
                    "first_any": nz_years[0] if nz_years else None,
                    "last": nz_years[-1] if nz_years else None,
                    "decade_density": {str(d): round(a / b, 3)
                                       for d, (a, b) in sorted(decade.items()) if b},
                    **({"out_of_span_values": [oos_nz[0], oos_nz[-1]]}
                       if oos_nz else {}),
                })
    finally:
        con.close()
    return out


def main() -> int:
    groups: dict[str, list] = defaultdict(list)
    for spec in WITNESS_MAP:
        groups[spec.source_key].append(spec)

    rows: list[dict] = []
    errors: list[dict] = []
    with ProcessPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_measure_source, item): item[0] for item in groups.items()}
        for future, source in [(f, s) for f, s in futures.items()]:
            try:
                for r in future.result():
                    (errors if "error" in r else rows).append(r)
            except Exception as exc:
                errors.append({"source": source, "error": repr(exc)})

    REACH.write_text(json.dumps({
        "artifact": "witness_reach",
        "measured": rows,
        "errors": errors,
        "notes": [
            "start = first SUSTAINED qualifying season: >=3 nonzero rows (>=1 when the table holds <30 rows that season), with another qualifying season within two years. A lone early value never opens a window; a padded zero never counts anywhere.",
            "first_any = old min-nonzero kept for comparison; decade_density = nonzero-row share per decade, the density scale showing when a source actually turns on.",
            "start null = no year axis, column absent, or no sustained coverage; the pivot falls back to the declared span and marks it approximate.",
        ],
    }, indent=1), encoding="utf-8")
    print(json.dumps({"reach": str(REACH), "measured": len(rows),
                      "errors": errors[:5], "error_count": len(errors)}, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
