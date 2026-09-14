"""
sota_recon/golden_witness_matrix.py -- golden-sample COMPLETENESS matrix (era x position x grain x witness).

For a curated roster spanning every era + position (Friedman/Nagurski/Hutson/Baugh/Lane/J.Brown/Sayers/
Butkus ... Mahomes/Hill/Henry/Donald/Tucker), show which STAT FAMILIES are actually populated at each
GRAIN -- weekly (super table), season, career (season_career artifacts) + whether the NFL.com witness
carries that player's season stats. This is the concrete completeness matrix: it shows what we can capture
for a 1927 QB vs a 2023 QB, and therefore HOW TO GATE each table per era (you can only gate an atom where a
witness provides it). Pure local parquet reads.

    python -m scripts.sota_recon.golden_witness_matrix
"""
from __future__ import annotations

import glob
from pathlib import Path

import duckdb

# (name, era_label, position, first_year) -- one representative per era/position cell
GOLDEN = [
    ("Benny Friedman", "1920s-30s", "QB", 1927),
    ("Bronko Nagurski", "1930s", "FB", 1930),
    ("Don Hutson", "1930s-40s", "WR", 1935),
    ("Sammy Baugh", "1930s-50s", "QB/DB/P", 1937),
    ("Night Train Lane", "1950s-60s", "DB", 1952),
    ("Jim Brown", "1950s-60s", "RB", 1957),
    ("Gale Sayers", "1960s-70s", "RB/KR", 1965),
    ("Dick Butkus", "1960s-70s", "LB", 1965),
    ("Walter Payton", "1970s-80s", "RB", 1975),
    ("Jerry Rice", "1980s-2000s", "WR", 1985),
    ("Derrick Henry", "2010s-20s", "RB", 2016),
    ("Tyreek Hill", "2010s-20s", "WR/KR", 2016),
    ("Patrick Mahomes", "2010s-20s", "QB", 2017),
    ("Aaron Donald", "2010s-20s", "DL/IDP", 2014),
    ("Justin Tucker", "2010s-20s", "K", 2012),
]

# stat family -> representative atom columns (present-if-any-nonzero)
FAMILIES = {
    "passing": ["passing_yards", "passing_tds", "attempts", "completions", "passing_interceptions"],
    "pass_adv": ["passing_first_downs", "passing_air_yards", "sacks_suffered", "passing_epa"],
    "rushing": ["rushing_yards", "rushing_tds", "carries"],
    "rush_adv": ["rushing_first_downs", "rushing_yards_before_contact", "rushing_broken_tackles"],
    "receiving": ["receptions", "receiving_yards", "receiving_tds", "targets"],
    "rec_adv": ["receiving_first_downs", "receiving_air_yards", "receiving_yac", "adot"],
    "fumbles": ["fumbles"],
    "fumbles_lost": ["fumbles_lost"],
    "scrimmage": ["yds_from_scrimmage", "all_purpose_yards", "total_tds_scored"],
    "defense": ["def_interceptions", "def_sacks", "fum_rec", "def_int_ret_td"],
    "def_adv": ["def_tackles_solo", "def_tackle_assists", "def_qb_hits", "def_pass_defended", "def_tackles_for_loss"],
    "kicking": ["fg_made", "fg_att", "pat_made"],
    "returns": ["kickoff_return_yards", "punt_return_yards", "kickoff_return_tds", "punt_return_tds"],
}

_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _latest_v26():
    return sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                  key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]


def _present_families(con, src, cols, where):
    """For each family, is ANY of its atoms nonzero for this player at this grain?"""
    out = {}
    for fam, atoms in FAMILIES.items():
        have = [a for a in atoms if a in cols]
        if not have:
            out[fam] = "-"  # no such column at this grain
            continue
        expr = " + ".join(_D(a) for a in have)
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{src}') WHERE {where} AND ({expr})<>0").fetchone()[0]
        out[fam] = "Y" if n else "."
    return out


def run():
    v26 = _latest_v26()
    art = Path(v26).parent / "season_career_v26"
    sea = (art / "player_nfl_season.parquet")
    car = (art / "player_nfl_career.parquet")
    con = duckdb.connect(); con.execute("SET memory_limit='2GB'"); con.execute("PRAGMA threads=3")
    wk_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{v26}')").fetchall()}
    sea_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{sea.as_posix()}')").fetchall()} if sea.exists() else set()
    car_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{car.as_posix()}')").fetchall()} if car.exists() else set()

    fams = list(FAMILIES)
    print("GOLDEN-SAMPLE COMPLETENESS MATRIX  (Y=populated  .=empty/absent  -=no column at grain)")
    print("grains: W=weekly(super)  S=season  C=career   | PBP-derivable = weekly 1978+ (epa/first-downs/adv)\n")
    hdr = "player               era        pos      G " + " ".join(f"{f[:8]:>8}" for f in fams)
    print(hdr); print("-" * len(hdr))
    for name, era, pos, fy in GOLDEN:
        esc = name.replace("'", "''")
        grains = [
            ("W", v26, wk_cols, f"player = '{esc}'"),
            ("S", sea.as_posix() if sea.exists() else None, sea_cols, f"player = '{esc}'"),
            ("C", car.as_posix() if car.exists() else None, car_cols, f"player = '{esc}'"),
        ]
        for gi, (g, src, cols, where) in enumerate(grains):
            if not src:
                continue
            pf = _present_families(con, src, cols, where)
            label = f"{name[:20]:20} {era:10} {pos:8}" if gi == 0 else " " * 40
            print(f"{label} {g} " + " ".join(f"{pf[f]:>8}" for f in fams))
        print()
    con.close()


if __name__ == "__main__":
    run()
