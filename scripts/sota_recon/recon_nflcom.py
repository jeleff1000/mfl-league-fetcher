"""
sota_recon/recon_nflcom.py -- reconcile the NFL.com witness against our super table (LOCAL D-drive files).

Maps every harvested NFL.com category column -> our canonical super-table atom (via
witness_contracts.NFLCOM_CATEGORY_ATOMS), then compares coverage era-by-era: where NFL.com AGREES with
what we already hold, where it FILLS a gap (super empty / shorter era), and which NFL.com atoms have no
super column at all (candidate backfills). Pure local parquet-vs-parquet -- no network.

    python -m scripts.sota_recon.recon_nflcom            # full mapping + coverage recon
    python -m scripts.sota_recon.recon_nflcom --atom def_interceptions   # value spot-check one atom
"""
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import duckdb

from .witness_contracts import NFLCOM_CATEGORY_ATOMS

NC = Path("D:/league-history-data/nfl/raw/nflcom/tables/player_season")
_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _super() -> str:
    return sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                  key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]


def _super_cov(con, sup, atom, lo, hi):
    """super-table nonzero-row count + era for `atom` within [lo,hi]."""
    try:
        r = con.execute(
            f"SELECT COUNT(*) FILTER (WHERE {_D(atom)}<>0), "
            f"MIN(year) FILTER (WHERE {_D(atom)}<>0), MAX(year) FILTER (WHERE {_D(atom)}<>0) "
            f"FROM read_parquet('{sup}') WHERE year BETWEEN {lo} AND {hi}"
        ).fetchone()
        return r
    except Exception:
        return None


def run() -> None:
    con = duckdb.connect(); con.execute("SET memory_limit='2GB'"); con.execute("PRAGMA threads=3")
    sup = _super()
    super_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sup}')").fetchall()}
    print("=" * 100)
    print("NFL.com  ->  SUPER TABLE  recon  (season-grain category pages; local parquet vs parquet)")
    print("=" * 100)
    new_atoms = []
    for p in sorted(NC.glob("*.parquet")):
        cat = p.stem
        amap = NFLCOM_CATEGORY_ATOMS.get(cat, {})
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p.as_posix()}')").fetchall()}
        yr = con.execute(f"SELECT MIN(TRY_CAST(season AS INT)), MAX(TRY_CAST(season AS INT)) FROM read_parquet('{p.as_posix()}')").fetchone()
        print(f"\n### {cat}   NFL.com era {yr[0]}-{yr[1]}")
        print(f"    {'nflcom col':16} {'-> super atom':26} {'in super?':9} {'super-nonzero (era)':22} verdict")
        for col in sorted(c for c in cols if not c.startswith('_') and c not in ('player', 'season', 'season_type')):
            atom = amap.get(col)
            if not atom:
                continue  # unmapped rate/aux col
            insuper = atom in super_cols
            if insuper:
                sc = _super_cov(con, sup, atom, yr[0], yr[1])
                cov = f"{sc[0]:,} ({sc[1]}-{sc[2]})" if sc and sc[0] else "0 (EMPTY)"
                verdict = "AGREE/validate" if sc and sc[0] else "NFL.com FILLS (super empty here)"
            else:
                cov = "-- no super col --"
                verdict = "NEW atom (candidate column)"
                new_atoms.append(f"{cat}.{atom}")
            print(f"    {col:16} {atom:26} {'yes' if insuper else 'NO':9} {cov:22} {verdict}")
    print("\n" + "=" * 100)
    print(f"NFL.com atoms with NO super column ({len(new_atoms)}): {', '.join(new_atoms)}")
    con.close()


def value_check(atom: str) -> None:
    """Spot-check agreement: NFL.com season total vs super-table season total for the same (player, year),
    for a handful of famous players -- validates the witness maps + agrees where both have data."""
    con = duckdb.connect(); con.execute("SET memory_limit='2GB'")
    sup = _super()
    # find which category maps to this atom
    cat = col = None
    for c, m in NFLCOM_CATEGORY_ATOMS.items():
        for k, v in m.items():
            if v == atom:
                cat, col = c, k
    if not cat:
        print(f"no NFL.com category maps to {atom}"); return
    p = (NC / f"{cat}.parquet").as_posix()
    # NFL.com category = SEASON grain; super = WEEKLY -> aggregate super to REG-season (SUM over REG weeks)
    # before comparing, and dedup NFL.com to one row per player-year.
    rows = con.execute(f"""
      WITH nc AS (SELECT player, TRY_CAST(season AS INT) yr, MAX({_D(col)}) nflcom
                  FROM read_parquet('{p}') WHERE season_type='reg' GROUP BY 1,2),
           s AS (SELECT player, CAST(year AS INT) yr, SUM({_D(atom)}) super
                 FROM read_parquet('{sup}')
                 WHERE UPPER(COALESCE(season_type,'REG')) IN ('REG','REGULAR') GROUP BY 1,2)
      SELECT nc.player, nc.yr, nc.nflcom, s.super, ABS(nc.nflcom-s.super) diff
      FROM nc JOIN s ON nc.player=s.player AND nc.yr=s.yr
      WHERE nc.nflcom>0 ORDER BY nc.nflcom DESC LIMIT 15""").fetchall()
    print(f"VALUE CHECK  {atom}  (NFL.com {cat}.{col} vs super, top season totals, joined on player+year):")
    print(f"  {'player':22} {'yr':>4} {'nflcom':>8} {'super':>8} {'diff':>6}")
    for r in rows:
        print(f"  {str(r[0])[:22]:22} {r[1]:>4} {r[2]:>8.0f} {r[3]:>8.0f} {r[4]:>6.0f}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--atom", default=None); a = ap.parse_args()
    if a.atom:
        value_check(a.atom)
    else:
        run()
