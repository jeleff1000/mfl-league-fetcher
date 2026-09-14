"""
sota_recon/recon_grid_gates.py -- structural GATES derived from the golden completeness grid.

The completeness grid (golden_completeness_grid) encodes structural truths -- each stat family's witness
era-floor and each nfl_position's real lifespan. Those truths become automated tripwires that catch the
SOTA failure mode a value-check can't: a stat that EXISTS but is IMPOSSIBLE for its era/position (a bad
join or a backfill fabricating/misattributing values).

GATES:
  ANACHRONISM   -- a family populated BEFORE its witness era-floor is impossible (no combine pre-2000, no
                   charted advanced stat pre-1978, no NGS pre-2016). A hit = fabrication/misattribution.
  POSITION_LIFE -- a player carrying nfl_position=X outside X's real era span (no NT pre-1974, no KR
                   post-2002) is a mislabel.
Both are era-aware and tolerate a tiny documented-edge count before failing.

    python -m scripts.sota_recon.recon_grid_gates
"""
from __future__ import annotations

import glob
from pathlib import Path

import duckdb

from .golden_completeness_grid import FAMILIES

_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"

# CONFIDENT per-atom anachronism floors = the TRUE earliest-witness era (calibrated to the actual source,
# NOT the PBP floor -- basic counting stats like fumbles/sacks have earlier box-score sources so they are
# NOT listed here). A nonzero super value before the floor is an impossible fabrication/misattribution.
CHARTED_FLOORS = {
    # charted air-yards / CPOE / YAC (nflverse charts from 2006)
    "receiving_air_yards": 2006, "passing_air_yards": 2006, "receiving_yac": 2006,
    "passing_yards_after_catch": 2006, "passing_cpoe": 2006, "receiving_adot": 2006,
    "passing_completed_air_yards": 2006, "receiving_completed_air_yards": 2006,
    # PFR advanced pass/rush/rec/def (2018)
    "passing_drops": 2018, "passing_pressured": 2018, "passing_hurried": 2018, "passing_hits": 2018,
    "rushing_yards_before_contact": 2018, "rushing_broken_tackles": 2018, "receiving_broken_tackles": 2018,
    "receiving_drops": 2018, "def_pressures": 2018, "def_hurries": 2018, "def_knockdowns": 2018,
    "def_blitzes": 2018, "def_tackles_missed": 2018,
    # snap counts (2012), NGS (2016), combine (2000)
    "offense_snaps": 2012, "defense_snaps": 2012, "special_teams_snaps": 2012,
    "ngs_avg_air_yards_to_sticks": 2016, "ngs_avg_air_yards_differential": 2016,
    "forty": 2000, "bench": 2000, "vertical": 2000, "broad_jump": 2000, "cone": 2000, "shuttle": 2000,
    # air_yards / YAC: EXHAUSTIVELY verified 2026-07-15 -- PBP air_yards+YAC both 2006+ (a 338-row 1999
    # YAC stray aside), NGS 2016+, PFR-adv 2018+, no play-description catch-point pre-2006, no derivation.
    # So pre-2006 super air_yards/YAC are SPURIOUS -> null them (confirmed).
    "receiving_yac": 2006, "yards_after_catch": 2006,
    # NOTE: fumbles_lost is NOT anachronistic pre-1978 -- PFR box-score bundle (enriched_through_1979)
    # legitimately carries it back past 1978. Removed from this gate (was a false positive).
}
ANACHRONISM_FLOORS = {}  # family bundles too coarse -> use CHARTED_FLOORS (per-atom, calibrated)
EXTRA_FLOORS = CHARTED_FLOORS

# nfl_position -> (first_year, last_year) real lifespan (from the super-table vocabulary census)
POSITION_LIFESPAN = {
    "E": (1920, 1974), "LE": (1920, 1969), "RE": (1920, 1962), "SE": (1959, 1969), "FL": (1953, 1982),
    "TB": (1920, 1952), "BB": (1920, 1952), "WB": (1920, 1952), "HB": (1920, 1973),
    "NT": (1974, 2026), "KR": (1986, 2003), "LILB": (1974, 2026), "ROLB": (1974, 2026),
    "LCB": (1960, 2026), "RCB": (1960, 2026), "OT": (2015, 2026), "OG": (2015, 2026), "MG": (1950, 1959),
    "LDH": (1950, 1960), "RDH": (1950, 1960),
}


def _latest_v26():
    return sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                  key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]


def run(tol=5):
    v26 = _latest_v26()
    con = duckdb.connect(); con.execute("SET memory_limit='3GB'"); con.execute("PRAGMA threads=3")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{v26}')").fetchall()}
    print("STRUCTURAL GATES FROM THE COMPLETENESS GRID")
    print("=" * 74)
    fails = 0

    print("\n[ANACHRONISM] rows with a family populated BEFORE its witness era-floor:")
    checks = []
    for fam, (atoms, floor) in ANACHRONISM_FLOORS.items():
        have = [a for a in atoms if a in cols]
        if have:
            checks.append((fam, " + ".join(_D(a) for a in have), floor))
    for atom, floor in EXTRA_FLOORS.items():
        if atom in cols:
            checks.append((atom, _D(atom), floor))
    for name, expr, floor in checks:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{v26}') WHERE year < {floor} AND ({expr})<>0").fetchone()[0]
        ok = n <= tol
        fails += 0 if ok else 1
        flag = "ok" if ok else "**FAIL**"
        if n > 0 or not ok:
            ex = con.execute(f"SELECT DISTINCT year FROM read_parquet('{v26}') WHERE year < {floor} AND ({expr})<>0 ORDER BY year LIMIT 5").fetchall()
            print(f"  [{flag}] {name:26} floor {floor}: {n} pre-floor rows  years={[int(e[0]) for e in ex]}")

    print("\n[POSITION_LIFESPAN] players with nfl_position outside its real era span:")
    if "nfl_position" in cols:
        for pos, (y0, y1) in POSITION_LIFESPAN.items():
            n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{v26}') WHERE nfl_position='{pos}' AND (year<{y0} OR year>{y1})").fetchone()[0]
            ok = n <= tol
            fails += 0 if ok else 1
            if n > 0:
                print(f"  [{'ok' if ok else '**FAIL**'}] {pos:6} span {y0}-{y1}: {n} out-of-era rows")

    con.close()
    print("\n" + "=" * 74)
    print(f"VERDICT: {'PASS' if fails == 0 else f'FAIL ({fails} gate(s))'}")
    return fails


if __name__ == "__main__":
    run()
