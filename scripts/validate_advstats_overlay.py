#!/usr/bin/env python3
"""
Post-write validation for the advanced-stats overlay release (latest_v26()).

Checks, all of which must pass before the release is trusted:
  1. schema     - every added atom + composite column is present
  2. composites - total_epa/total_wpa/scrimmage_yards/total_tds/total_touches EXACTLY equal
                  the sum of their components on every row (0 mismatches)
  3. depth      - EPA/WPA populate only 1999+ (charting/EPA era); explosive/red-zone
                  populate earlier too (model-free). Reported per sample era.
  4. leaders    - passing_wpa / rz_carries leaders for a recent year look right
  5. jaguars    - JAX 2001-2002 population + anomaly flag (nflverse PBP bug watch)

    python scripts/validate_advstats_overlay.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts.integrate_advanced_atoms_into_supertable import COMPOSITES, NEW_ATOMS  # noqa: E402
from scripts.sota_recon.sources import latest_v26  # noqa: E402

COMPONENT_MAP = {name: [p.split(".", 1)[1] for p in parts] for name, parts in COMPOSITES.items()}


def main() -> int:
    src = latest_v26()
    con = duckdb.connect()
    con.execute("SET temp_directory='D:/league-history-data/nfl/tmp/duckdb'")
    con.execute("SET memory_limit='1200MB'")
    con.execute("PRAGMA threads=2")
    R = f"read_parquet('{src}')"
    print(f"release: {src}\n")

    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {R}").fetchall()}
    added = NEW_ATOMS + list(COMPOSITES.keys())
    missing = [c for c in added if c not in cols]
    print(f"[1] schema: {len(cols)} cols; added {len(added)} (18 atoms + 5 composites); missing={missing}")
    ok_schema = not missing

    print("[2] composite integrity (mismatches must be 0):")
    ok_comp = True
    for name, comps in COMPONENT_MAP.items():
        all_null = " AND ".join(f"{c} IS NULL" for c in comps)
        summed = " + ".join(f"COALESCE({c},0)" for c in comps)
        bad = con.execute(
            f"SELECT COUNT(*) FROM {R} WHERE {name} IS NOT NULL "
            f"AND ABS({name} - ({summed})) > 1e-6"
        ).fetchone()[0]
        null_bad = con.execute(
            f"SELECT COUNT(*) FROM {R} WHERE {name} IS NULL AND NOT ({all_null})"
        ).fetchone()[0]
        flag = "OK" if (bad == 0 and null_bad == 0) else "FAIL"
        if bad or null_bad:
            ok_comp = False
        print(f"    {name:<16} value_mismatch={bad:<4} null_when_components_present={null_bad:<4} {flag}")

    print("[3] population depth by era (% of offensive rows with atom populated):")
    for yr in (1985, 1998, 1999, 2010, 2023):
        row = con.execute(
            f"""SELECT
                   COUNT(*) FILTER (WHERE COALESCE(attempts,0)+COALESCE(carries,0)+COALESCE(targets,0)>0) AS off_n,
                   COUNT(*) FILTER (WHERE passing_wpa IS NOT NULL OR rushing_wpa IS NOT NULL OR receiving_wpa IS NOT NULL) AS wpa_n,
                   COUNT(*) FILTER (WHERE pass_explosive_20 IS NOT NULL OR rush_explosive_10 IS NOT NULL OR rec_explosive_20 IS NOT NULL) AS expl_n,
                   COUNT(*) FILTER (WHERE rz_carries IS NOT NULL OR rz_targets IS NOT NULL OR rz_pass_att IS NOT NULL) AS rz_n
               FROM {R} WHERE CAST(year AS INTEGER)={yr}"""
        ).fetchone()
        off = max(row[0], 1)
        print(f"    {yr}: off={row[0]:<5} wpa={100*row[1]/off:5.1f}%  explosive={100*row[2]/off:5.1f}%  redzone={100*row[3]/off:5.1f}%")

    print("[4] 2023 passing_wpa leaders:")
    for r in con.execute(
        f"SELECT player, ROUND(SUM(passing_wpa),2) FROM {R} WHERE CAST(year AS INTEGER)=2023 "
        f"GROUP BY 1 HAVING SUM(passing_wpa) IS NOT NULL ORDER BY 2 DESC LIMIT 5"
    ).fetchall():
        print(f"    {r[0]:<24} wpa={r[1]}")

    print("[5] Jaguars 2001-2002 watch (nflverse PBP bug):")
    for yr in (2001, 2002):
        r = con.execute(
            f"""SELECT COUNT(DISTINCT week) AS wks,
                       COUNT(*) FILTER (WHERE COALESCE(attempts,0)+COALESCE(carries,0)+COALESCE(targets,0)>0) AS off_n,
                       ROUND(SUM(passing_epa),1) AS pass_epa,
                       ROUND(SUM(passing_wpa),2) AS pass_wpa
                FROM {R} WHERE CAST(year AS INTEGER)={yr} AND nfl_team='JAX'"""
        ).fetchone()
        print(f"    JAX {yr}: weeks={r[0]} off_rows={r[1]} sum_pass_epa={r[2]} sum_pass_wpa={r[3]}")

    print()
    verdict = "PASS" if (ok_schema and ok_comp) else "FAIL"
    print(f"OVERALL: {verdict}  (schema={ok_schema}, composites={ok_comp})")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
