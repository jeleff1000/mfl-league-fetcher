"""
sota_recon/recon_bounds.py  --  LANE: player-grain HARD-INVARIANT bound contracts.

Phase 1a of the SOTA closeout plan (WS4 contract type 1). These bounds hold in every era,
need no witness, and a violation is a defect by definition -- the Masterson cell
(9 passing INTs on 4 attempts) fails here instantly. Only HARD invariants live in this
lane; conditional invariants (e.g. long==total when event_count==1) and plausibility
ceilings are separate certainty types and do NOT gate builds from here.

NOTE deliberately absent: `*_long <= *_yards` is NOT a valid hard bound (a 40-yard long
plus a -5-yard play gives total 35 < long 40). See the plan doc, WS4.

    python -m scripts.sota_recon.recon_bounds              # summary to stdout
    python -m scripts.sota_recon.recon_bounds --csv out.csv # + violating rows
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from .sources import latest_v26

# (name, child_expr, parent_expr) -- violation when child > parent, both non-NULL.
HARD_BOUNDS = [
    ("passing_int_le_attempts", "passing_interceptions", "attempts"),
    ("completions_le_attempts", "completions", "attempts"),
    ("passing_tds_le_completions", "passing_tds", "completions"),
    ("receptions_le_targets", "receptions", "targets"),
    ("receiving_tds_le_receptions", "receiving_tds", "receptions"),
    ("rushing_tds_le_carries", "rushing_tds", "carries"),
    ("fg_made_le_fg_att", "fg_made", "fg_att"),
    ("pat_made_le_pat_att", "pat_made", "pat_att"),
    # wave52/54 columns. NOTE: completed_air <= intended_air is NOT a valid bound --
    # air yards go negative behind the LOS, so an incompletion at -3 can drag intended
    # below completed; the relationship is not monotone.
    ("fg60_le_fg_made", "fg_made_60plus", "fg_made"),
]


def violation_sql(src: str) -> str:
    parts = []
    for name, child, parent in HARD_BOUNDS:
        parts.append(f"""
        SELECT '{name}' AS bound, player_week, year, week, nfl_team, position,
               {child} AS child_value, {parent} AS parent_value
        FROM '{src}'
        WHERE {child} IS NOT NULL AND {parent} IS NOT NULL AND {child} > {parent}""")
    return " UNION ALL ".join(parts)


def run(src: str | None = None, csv: str | None = None) -> dict:
    vq = Path(src or latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute(f"CREATE TEMP TABLE v AS {violation_sql(vq)}")
    by_bound = dict(con.execute(
        "SELECT bound, COUNT(*) FROM v GROUP BY bound ORDER BY 2 DESC").fetchall())
    by_era = con.execute("""
        SELECT bound, MIN(year) AS min_yr, MAX(year) AS max_yr, COUNT(*) AS n
        FROM v GROUP BY bound ORDER BY n DESC""").fetchall()
    total = sum(by_bound.values())
    if csv:
        con.execute(f"COPY (SELECT * FROM v ORDER BY bound, year, week) TO '{csv}' (HEADER)")
    con.close()
    return {"total": total, "by_bound": by_bound, "by_era": by_era, "csv": csv}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default=None)
    ap.add_argument("--csv", default=None)
    a = ap.parse_args()
    r = run(a.src, a.csv)
    print(f"HARD BOUND VIOLATIONS: {r['total']}")
    for name, n in r["by_bound"].items():
        print(f"  {name:32s} {n:6d}")
    for bound, mn, mx, n in r["by_era"]:
        print(f"  {bound:32s} years {mn}-{mx}  n={n}")
    if r["csv"]:
        print("rows ->", r["csv"])
