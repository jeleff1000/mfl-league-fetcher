"""
sota_recon/recon_defense_conservation.py -- DEFENSE/IDP conservation: DST row == Sigma(individual defenders).

The team-DST row and the individual IDP rows are two views of the SAME events: if Ed Reed picks off a pass
for the Ravens, he has 1 INT AND the Ravens DST has 1 INT. So for every (franchise, year):

    DST-row defensive atom  ==  SUM over individual defenders of that atom

A mismatch localizes a real bug that per-row checks miss:
  * DST > Sigma(IDP)  -> the DST line double-counts (e.g. a return TD credited to DST AND a returner) or an
                         IDP player-row is missing.
  * DST < Sigma(IDP)  -> the DST line under-counts / an IDP credit isn't rolled into the team line.
recon_vertical_team already does this for OFFENSE (pass/rush); this is the missing DEFENSE lane, and it's
the direct guard against the DST-double-counts-IDP class the A1 return-TD work touched.

Atoms: def_interceptions, def_sacks, fum_rec, def_int_ret_td, def_safeties. (def_tds via GREATEST dedup.)

    python -m scripts.sota_recon.recon_defense_conservation [--year 2004] [--tol 0]
"""
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import duckdb

_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"
ATOMS = ["def_interceptions", "def_sacks", "fum_rec", "def_int_ret_td", "def_safeties"]


def _latest_v26():
    return sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                  key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]


def run(year=None, tol=0.0, show=25):
    v26 = _latest_v26()
    con = duckdb.connect(); con.execute("SET memory_limit='3GB'"); con.execute("PRAGMA threads=3")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{v26}')").fetchall()}
    atoms = [a for a in ATOMS if a in cols]
    yfilter = f"AND year={year}" if year else ""
    dst_sum = ", ".join(f"SUM({_D(a)}) AS d_{a}" for a in atoms)
    idp_sum = ", ".join(f"SUM({_D(a)}) AS i_{a}" for a in atoms)
    q = f"""
    WITH dst AS (
      SELECT TRY_CAST(nfl_franchise_number AS INT) fid, year, UPPER(COALESCE(season_type,'REG')) st,
             ANY_VALUE(nfl_team) code, {dst_sum}
      FROM read_parquet('{v26}') WHERE position='DEF' AND nfl_franchise_number IS NOT NULL {yfilter}
      GROUP BY 1,2,3
    ),
    idp AS (  -- individual defenders (everything not the team-DST row) carrying defensive atoms
      SELECT TRY_CAST(nfl_franchise_number AS INT) fid, year, UPPER(COALESCE(season_type,'REG')) st, {idp_sum}
      FROM read_parquet('{v26}') WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL {yfilter}
      GROUP BY 1,2,3
    )
    SELECT d.fid, d.year, d.st, d.code,
           {', '.join(f'd.d_{a}, COALESCE(i.i_{a},0) i_{a}, (d.d_{a}-COALESCE(i.i_{a},0)) delta_{a}' for a in atoms)}
    FROM dst d LEFT JOIN idp i ON d.fid=i.fid AND d.year=i.year AND d.st=i.st
    """
    rows = con.execute(q).fetchall()
    # tally mismatches per atom
    ncol = 4
    idx = {}
    for a in atoms:
        idx[a] = (ncol, ncol + 1, ncol + 2); ncol += 3
    tally = {a: {"rows": 0, "mismatch": 0, "dst_gt": 0, "dst_lt": 0, "sum_absdelta": 0.0} for a in atoms}
    worst = {a: [] for a in atoms}
    for r in rows:
        for a in atoms:
            di, ii, xi = idx[a]
            dv, iv, dd = r[di], r[ii], r[xi]
            if r[2] != "REG":
                continue
            tally[a]["rows"] += 1
            if abs(dd) > tol:
                tally[a]["mismatch"] += 1
                tally[a]["sum_absdelta"] += abs(dd)
                if dd > 0:
                    tally[a]["dst_gt"] += 1
                else:
                    tally[a]["dst_lt"] += 1
                worst[a].append((abs(dd), r[3], int(r[1]), dv, iv, dd))
    con.close()
    print("DEFENSE/IDP CONSERVATION  (REG; DST row vs Sigma individual defenders, per franchise-year)")
    print(f"{'atom':22}{'yrs':>6}{'mismatch':>10}{'DST>IDP':>9}{'DST<IDP':>9}{'Sig|delta|':>11}")
    for a in atoms:
        t = tally[a]
        print(f"{a:22}{t['rows']:>6}{t['mismatch']:>10}{t['dst_gt']:>9}{t['dst_lt']:>9}{t['sum_absdelta']:>11.0f}")
    for a in atoms:
        if worst[a]:
            print(f"\ntop {a} mismatches (team, yr, DST, IDPsum, delta):")
            for w in sorted(worst[a], reverse=True)[:show // len(atoms) + 3]:
                print(f"  {w[1]:>5} {w[2]}  DST={w[3]:.0f} IDPsum={w[4]:.0f} delta={w[5]:+.0f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=None)
    ap.add_argument("--tol", type=float, default=0.0)
    a = ap.parse_args()
    run(year=a.year, tol=a.tol)
