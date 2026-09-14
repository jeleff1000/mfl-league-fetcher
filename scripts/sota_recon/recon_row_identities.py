"""
sota_recon/recon_row_identities.py  --  LANE: per-ROW cell identities (every row, not outliers).

Bounds/ceilings catch impossible extremes; conservation catches aggregate drift. Neither
verifies an ORDINARY cell. This lane does, for every cell that is redundantly derivable
from other cells in the SAME row: the derived cell must equal its formula exactly, on
every row in the table. A mismatch localizes to a specific (row, cell) pair -- per-cell
verification of ~10 columns x 1.2M rows with zero witnesses needed.

This is the in-row arm of the WS4c cell-verdict layer. The witness arm (oracle-style
per-cell equality vs PFA/boxscore/nflverse) and the cross-grain arm (season cell == sum of
weekly cells) extend the same verdict model to non-derivable cells.

    python -m scripts.sota_recon.recon_row_identities [--csv out.csv]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from .sources import latest_v26

# (check_name, derived_col, formula_sql, guard_sql, abs_tolerance)
# UNITS ARE PER-COLUMN, NOT PER-SUFFIX (measured 2026-07-10): completion_pct/catch_pct are
# 0-100 scale but fg_pct/pat_pct are 0-1 scale in the same table. The formula encodes each
# column's registered unit; the unit inconsistency itself is queued as a defect/registry item.
IDENTITIES = [
    ("fg_missed_eq", "fg_missed", "fg_att - fg_made",
     "fg_att IS NOT NULL AND fg_made IS NOT NULL", 0.0),
    ("pat_missed_eq", "pat_missed", "pat_att - pat_made",
     "pat_att IS NOT NULL AND pat_made IS NOT NULL", 0.0),
    ("completion_pct_eq", "completion_pct", "100.0 * completions / attempts",
     "attempts > 0 AND completions IS NOT NULL", 0.05),
    ("catch_pct_eq", "catch_pct", "100.0 * receptions / targets",
     "targets > 0 AND receptions IS NOT NULL", 0.05),
    ("fg_pct_eq", "fg_pct", "fg_made * 1.0 / fg_att",
     "fg_att > 0 AND fg_made IS NOT NULL", 0.0005),
    ("pat_pct_eq", "pat_pct", "pat_made * 1.0 / pat_att",
     "pat_att > 0 AND pat_made IS NOT NULL", 0.0005),
    ("pass_ypa_eq", "passing_yards_per_attempt", "passing_yards * 1.0 / attempts",
     "attempts > 0 AND passing_yards IS NOT NULL", 0.05),
    ("rush_ypc_eq", "rushing_yards_per_carry", "rushing_yards * 1.0 / carries",
     "carries > 0 AND rushing_yards IS NOT NULL", 0.05),
    ("recv_ypr_eq", "receiving_yards_per_reception", "receiving_yards * 1.0 / receptions",
     "receptions > 0 AND receiving_yards IS NOT NULL", 0.05),
    ("recv_ypt_eq", "receiving_yards_per_target", "receiving_yards * 1.0 / targets",
     "targets > 0 AND receiving_yards IS NOT NULL", 0.05),
    ("punt_ypp_eq", "punt_yards_per_punt", "punt_yards * 1.0 / punts",
     "punts > 0 AND punt_yards IS NOT NULL", 0.05),
    # wave52 composites: every derived column self-verifies forever
    ("touches_eq", "touches", "COALESCE(carries, 0) + COALESCE(receptions, 0)",
     "touches IS NOT NULL", 0.0),
    ("opportunities_eq", "opportunities", "COALESCE(carries, 0) + COALESCE(targets, 0)",
     "opportunities IS NOT NULL", 0.0),
    ("turnovers_eq", "turnovers",
     "COALESCE(passing_interceptions, 0) + COALESCE(fumbles_lost, 0)",
     "turnovers IS NOT NULL", 0.0),
    ("scrimmage_tds_eq", "scrimmage_tds",
     "COALESCE(rushing_tds, 0) + COALESCE(receiving_tds, 0)",
     "scrimmage_tds IS NOT NULL", 0.0),
    ("total_return_yards_eq", "total_return_yards",
     "COALESCE(kickoff_return_yards, 0) + COALESCE(punt_return_yards, 0)",
     "total_return_yards IS NOT NULL", 0.0),
    ("def_tackles_combined_eq", "def_tackles_combined",
     "COALESCE(def_tackles_solo, 0) + COALESCE(def_tackle_assists, 0)",
     "def_tackles_combined IS NOT NULL", 0.0),
    ("dropbacks_eq", "dropbacks", "COALESCE(attempts, 0) + COALESCE(sacks_suffered, 0)",
     "dropbacks IS NOT NULL", 0.0),
    ("all_purpose_yards_eq", "all_purpose_yards",
     "COALESCE(rushing_yards, 0) + COALESCE(receiving_yards, 0) "
     "+ COALESCE(kickoff_return_yards, 0) + COALESCE(punt_return_yards, 0) "
     "+ COALESCE(def_interception_yards, 0) + COALESCE(fum_rec_yds, 0)",
     "all_purpose_yards IS NOT NULL", 0.0),
    ("yards_per_touch_eq", "yards_per_touch",
     "(COALESCE(rushing_yards, 0) + COALESCE(receiving_yards, 0)) * 1.0 "
     "/ (COALESCE(carries, 0) + COALESCE(receptions, 0))",
     "COALESCE(carries, 0) + COALESCE(receptions, 0) > 0 "
     "AND yards_per_touch IS NOT NULL", 0.05),
    ("fg60_eq", "fg_made_60plus",
     "fg_made - (fg_made_0_19 + fg_made_20_29 + fg_made_30_39 "
     "+ fg_made_40_49 + fg_made_50_59)",
     "fg_made IS NOT NULL AND fg_made_0_19 IS NOT NULL AND fg_made_20_29 IS NOT NULL "
     "AND fg_made_30_39 IS NOT NULL AND fg_made_40_49 IS NOT NULL "
     "AND fg_made_50_59 IS NOT NULL AND fg_made_60plus IS NOT NULL", 0.0),
]


def run(src: str | None = None, csv: str | None = None) -> dict:
    vq = Path(src or latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    per_check, samples = {}, []
    for name, col, formula, guard, tol in IDENTITIES:
        checked, bad = con.execute(f"""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE ABS({col} - ({formula})) > {tol})
            FROM '{vq}' WHERE {guard} AND {col} IS NOT NULL""").fetchone()
        per_check[name] = {"cells_checked": checked, "mismatches": bad}
        if bad:
            samples += [dict(check=name, player_week=k, year=y, stored=s, formula_value=f)
                        for k, y, s, f in con.execute(f"""
                SELECT player_week, year, {col}, {formula} FROM '{vq}'
                WHERE {guard} AND {col} IS NOT NULL AND ABS({col} - ({formula})) > {tol}
                ORDER BY ABS({col} - ({formula})) DESC LIMIT 25""").fetchall()]
    con.close()

    total_checked = sum(v["cells_checked"] for v in per_check.values())
    total_bad = sum(v["mismatches"] for v in per_check.values())
    if csv and samples:
        import csv as _csv
        with open(csv, "w", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=list(samples[0]))
            w.writeheader(); w.writerows(samples)
    return {"cells_checked": total_checked, "mismatched_cells": total_bad,
            "per_check": per_check, "samples": samples}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    ap.add_argument("--src", default=None)
    a = ap.parse_args()
    r = run(a.src, a.csv)
    print(f"PER-CELL IDENTITY CHECK: {r['cells_checked']:,} cells verified, "
          f"{r['mismatched_cells']:,} mismatches")
    for name, v in r["per_check"].items():
        flag = "  <-- " if v["mismatches"] else ""
        print(f"  {name:22s} checked={v['cells_checked']:>9,}  bad={v['mismatches']:,}{flag}")
    for s in r["samples"][:10]:
        print(f"    {s['check']}: {s['player_week']} stored={s['stored']} "
              f"formula={round(s['formula_value'],2)}")
