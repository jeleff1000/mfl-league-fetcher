"""Audit PFR rate witnesses against their source counters for 2025.

This is deliberately source-native: it checks the PFR published rate against the
PFR numerator/denominator in the same registered table.  It does not promote a
rate into the supertable and does not use a season rate as a counter.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr\players\tables")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-denominator-audit-2025.json")
TOL = 0.15

SPECS = {
    "passing": {
        "pass_cmp_pct": ("pass_cmp", "pass_att", "100.0"),
        "pass_yds_per_att": ("pass_yds", "pass_att", "1.0"),
        "pass_yds_per_cmp": ("pass_yds", "pass_cmp", "1.0"),
        "pass_yds_per_g": ("pass_yds", "games", "1.0"),
        "pass_td_pct": ("pass_td", "pass_att", "100.0"),
        "pass_int_pct": ("pass_int", "pass_att", "100.0"),
        # PFR defines sack percentage over dropbacks, not attempts alone.
        "pass_sacked_pct": ("pass_sacked", "pass_att + pass_sacked", "100.0"),
    },
    "rushing_and_receiving": {
        "rush_yds_per_att": ("rush_yds", "rush_att", "1.0"),
        "rush_yds_per_g": ("rush_yds", "games", "1.0"),
        "rush_att_per_g": ("rush_att", "games", "1.0"),
        "rec_yds_per_rec": ("rec_yds", "rec", "1.0"),
        "rec_per_g": ("rec", "games", "1.0"),
        "rec_yds_per_g": ("rec_yds", "games", "1.0"),
        "catch_pct": ("rec", "targets", "100.0"),
        "rec_yds_per_tgt": ("rec_yds", "targets", "1.0"),
    },
    "receiving_and_rushing": {
        "rec_yds_per_rec": ("rec_yds", "rec", "1.0"),
        "rec_per_g": ("rec", "games", "1.0"),
        "rec_yds_per_g": ("rec_yds", "games", "1.0"),
        "catch_pct": ("rec", "targets", "100.0"),
        "rec_yds_per_tgt": ("rec_yds", "targets", "1.0"),
        "rush_yds_per_att": ("rush_yds", "rush_att", "1.0"),
        "rush_yds_per_g": ("rush_yds", "games", "1.0"),
        "rush_att_per_g": ("rush_att", "games", "1.0"),
    },
    "scoring": {"points_per_g": ("scoring", "games", "1.0")},
}


def main() -> None:
    con = duckdb.connect()
    checks = []
    for table, rates in SPECS.items():
        path = str(ROOT / table / "_combined.parquet").replace("\\", "/")
        cols = set(con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchdf()["column_name"])
        for rate, (numerator, denominator, multiplier) in rates.items():
            required = {rate, numerator} | {x.strip() for x in denominator.split("+")}
            if not required <= cols:
                checks.append({"table": table, "rate": rate, "status": "SKIPPED_MISSING_OPERAND"})
                continue
            n = f"TRY_CAST({numerator} AS DOUBLE)"
            d = " + ".join(f"TRY_CAST({x.strip()} AS DOUBLE)" for x in denominator.split("+"))
            q = f"""
                SELECT
                  COUNT(*) FILTER (WHERE {rate} IS NOT NULL AND {n} IS NOT NULL AND {d} IS NOT NULL AND {d} <> 0) AS comparable,
                  COUNT(*) FILTER (WHERE {rate} IS NOT NULL AND {n} IS NOT NULL AND {d} IS NOT NULL AND {d} <> 0
                    AND ABS(TRY_CAST({rate} AS DOUBLE) - ({n} / ({d}) * {multiplier})) <= {TOL}) AS matches
                FROM read_parquet(?) WHERE year_id = '2025'
            """
            comparable, matches = con.execute(q, [path]).fetchone()
            comparable = int(comparable or 0)
            matches = int(matches or 0)
            checks.append({
                "table": table, "rate": rate, "numerator": numerator,
                "denominator": denominator, "comparable": comparable,
                "matches": matches, "mismatches": comparable - matches,
                "status": "PASS" if comparable == matches else "FAIL",
            })
    con.close()
    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "year": 2025, "tolerance": TOL,
        "checks": checks,
        "summary": {
            "checks": len(checks),
            "failed": sum(x.get("status") == "FAIL" for x in checks),
            "skipped": sum(x.get("status", "").startswith("SKIPPED") for x in checks),
        },
        "notes": [
            "Rates are witnesses; the denominator is checked against the same-table PFR counter.",
            "pass_sacked_pct uses dropbacks (pass_att + pass_sacked), not pass attempts alone.",
        ],
    }
    OUT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(out["summary"])


if __name__ == "__main__":
    main()
