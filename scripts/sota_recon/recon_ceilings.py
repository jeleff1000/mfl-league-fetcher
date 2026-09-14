"""
sota_recon/recon_ceilings.py  --  LANE: published-record ceilings + physical play ceilings (WS7a).

No cell may exceed the PUBLISHED all-time record for its stat x grain. Masterson's 9 passing
INTs > Jim Hardy's published 8 is the proof case: golden_matrix checks marquee leaders, but a
wrong value on a stat with no grid entry sails through. Ceilings are one number per stat --
cheap to curate, devastating to bad extremes.

Anchor policy (WS7a freeze discipline): every ceiling carries a citation. Record ceilings are
REG-season and CAN be legitimately broken -- a new exceedance is a review/build event, not an
auto-defect. Physical ceilings (a scrimmage play cannot gain more than 99 yards) hold for all
season types and eras; exceedance is a hard defect.

Seeds import golden_matrix's vetted record book (RECORDS/DEF_RECORDS/WEEK_CEILING) so the two
lanes cannot drift apart; NEW_CEILINGS adds stats the grid lacks.

    python -m scripts.sota_recon.recon_ceilings [--csv out.csv]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from .golden_matrix import DEF_RECORDS, RECORDS, WEEK_CEILING
from .sources import latest_v26

# (grain, stat, ceiling, citation) -- record ceilings the golden grid does NOT carry
NEW_CEILINGS = [
    ("week", "passing_interceptions", 8, "Jim Hardy 1950-09-24 CHI@CRD (PFR single-game record)"),
    ("season", "passing_interceptions", 42, "George Blanda 1962 HOU (PFR season record)"),
    ("career", "passing_interceptions", 336, "Brett Favre (PFR career record)"),
    ("week", "def_interceptions", 4, "NFL single-game record (multiple holders)"),
    ("week", "fg_long", 66, "Justin Tucker 2021-09-26 (NFL record)"),
]

# physical ceilings: field geometry, all season types. A scrimmage play maxes at 99 yards;
# a return that starts 9 yards deep in the end zone maxes at 109.
PHYSICAL_CEILINGS = [
    ("week", "passing_long", 99, "physical: max scrimmage play"),
    ("week", "rushing_long", 99, "physical: max scrimmage play"),
    ("week", "receiving_long", 99, "physical: max scrimmage play"),
    ("week", "kickoff_return_long", 109, "physical: max return"),
    ("week", "punt_return_long", 109, "physical: max return"),
]


def _anchor_rows() -> list[tuple[str, str, float, str, str]]:
    """(grain, stat, ceiling, citation, klass) from golden seeds + additions."""
    rows = []
    for grain, stat, value, holder, st in RECORDS + DEF_RECORDS:
        if st != "REG":
            continue
        rows.append((grain, stat, float(value), f"record: {holder or 'multiple holders'}", "RECORD"))
    for stat, value in WEEK_CEILING.items():
        rows.append(("week", stat, float(value), "record: golden_matrix WEEK_CEILING", "RECORD"))
    for grain, stat, value, cite in NEW_CEILINGS:
        rows.append((grain, stat, float(value), cite, "RECORD"))
    for grain, stat, value, cite in PHYSICAL_CEILINGS:
        rows.append((grain, stat, float(value), cite, "PHYSICAL"))
    return rows


def run(src: str | None = None, csv: str | None = None) -> dict:
    vq = Path(src or latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}' LIMIT 0").fetchall()}

    violations, skipped = [], []
    for grain, stat, ceiling, cite, klass in _anchor_rows():
        if stat not in have:
            skipped.append((grain, stat))
            continue
        # player records: team-DST rows (position='DEF', ids like DEF-5) are team totals
        # and legitimately dwarf any individual record -- they get their own anchor class.
        st_filter = "AND COALESCE(position,'') <> 'DEF'"
        if klass == "RECORD":
            st_filter += " AND season_type = 'REG'"
        if grain == "week":
            q = f"""SELECT player_week AS key, {stat} AS val FROM '{vq}'
                    WHERE {stat} IS NOT NULL {st_filter} AND {stat} > {ceiling}
                    ORDER BY val DESC LIMIT 20"""
        elif grain == "season":
            q = f"""SELECT NFL_player_id || '_' || year AS key, SUM({stat}) AS val FROM '{vq}'
                    WHERE {stat} IS NOT NULL {st_filter}
                    GROUP BY NFL_player_id, year HAVING SUM({stat}) > {ceiling}
                    ORDER BY val DESC LIMIT 20"""
        else:
            q = f"""SELECT NFL_player_id AS key, SUM({stat}) AS val FROM '{vq}'
                    WHERE {stat} IS NOT NULL {st_filter}
                    GROUP BY NFL_player_id HAVING SUM({stat}) > {ceiling}
                    ORDER BY val DESC LIMIT 20"""
        for key, val in con.execute(q).fetchall():
            violations.append(dict(klass=klass, grain=grain, stat=stat, ceiling=ceiling,
                                   value=val, key=key, citation=cite))
    con.close()
    if csv and violations:
        import csv as _csv
        with open(csv, "w", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=list(violations[0]))
            w.writeheader(); w.writerows(violations)
    return {"total": len(violations), "violations": violations, "skipped_missing_cols": skipped}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    ap.add_argument("--src", default=None)
    a = ap.parse_args()
    r = run(a.src, a.csv)
    print(f"CEILING VIOLATIONS: {r['total']}")
    for v in r["violations"]:
        print(f"  [{v['klass']:8s}] {v['grain']:6s} {v['stat']:28s} "
              f"value={v['value']:<8g} ceiling={v['ceiling']:<6g} {v['key']}  ({v['citation']})")
    if r["skipped_missing_cols"]:
        print("skipped (column absent):", r["skipped_missing_cols"])
