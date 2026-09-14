"""Regression tripwire of known-truth anchors for the local v26 table."""

from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq

from .sources import latest_v26

RECORD_MAX = {
    "rushing_yards": 296,
    "passing_yards": 554,
    "receiving_yards": 336,
    "receptions": 21,
    "passing_tds": 7,
    "rushing_tds": 6,
    "receiving_tds": 5,
    "fg_made": 8,
    "attempts": 70,
    "completions": 54,
}

SEASON_TOTAL = [
    ("Eric Dickerson", 1984, "rushing_yards", 2105),
    ("Dan Marino", 1984, "passing_yards", 5084),
    ("Jerry Rice", 1995, "receiving_yards", 1848),
    ("Marvin Harrison", 2002, "receptions", 143),
    ("LaDainian Tomlinson", 2006, "rushing_tds", 28),
    ("Peyton Manning", 2013, "passing_tds", 55),
    ("Peyton Manning", 2013, "passing_yards", 5477),
    ("Patrick Mahomes", 2018, "passing_tds", 50),
    ("Tom Brady", 2007, "passing_tds", 50),
    ("Drew Brees", 2011, "passing_yards", 5476),
    ("Adrian Peterson", 2012, "rushing_yards", 2097),
    ("Barry Sanders", 1997, "rushing_yards", 2053),
    ("Derrick Henry", 2020, "rushing_yards", 2027),
    ("Calvin Johnson", 2012, "receiving_yards", 1964),
    ("Cooper Kupp", 2021, "receptions", 145),
    ("Cooper Kupp", 2021, "receiving_yards", 1947),
    ("Michael Thomas", 2019, "receptions", 149),
    ("Randy Moss", 2007, "receiving_tds", 23),
    ("Rob Gronkowski", 2011, "receiving_tds", 17),
    ("Jerry Rice", 1987, "receiving_tds", 22),
]

CAREER_TOTAL = [
    ("Jerry Rice", "receptions", 1549),
    ("Jerry Rice", "receiving_yards", 22895),
    ("Jerry Rice", "receiving_tds", 197),
    ("Emmitt Smith", "rushing_yards", 18355),
    ("Emmitt Smith", "rushing_tds", 164),
    ("Walter Payton", "rushing_yards", 16726),
    ("Walter Payton", "rushing_tds", 110),
    ("Peyton Manning", "passing_yards", 71940),
    ("Peyton Manning", "passing_tds", 539),
    ("Brett Favre", "passing_tds", 508),
    ("Drew Brees", "passing_yards", 80358),
    ("Drew Brees", "passing_tds", 571),
    ("Tom Brady", "passing_yards", 89214),
    ("Tom Brady", "passing_tds", 649),
]

DEF_SEASON_REG = [
    ("T.J. Watt", 2021, "def_sacks", 22.5),
    ("Jared Allen", 2011, "def_sacks", 22.0),
    ("Aaron Donald", 2018, "def_sacks", 20.5),
]
KICKER_SEASON = [("David Akers", 2011, "fg_made", 44)]
PRE1978_TOTAL = [
    ("Sammy Baugh", 1947, "passing_yards", 2938),
    ("Jim Brown", 1963, "rushing_yards", 1863),
    ("Don Hutson", 1942, "receiving_yards", 1211),
    ("O.J. Simpson", 1973, "rushing_yards", 2003),
]
DEF_SEASON = [
    ("Michael Strahan", 2001, "def_sacks", 22.5),
    ("Mark Gastineau", 1984, "def_sacks", 22),
    ("Night Train Lane", 1952, "def_interceptions", 14),
]


def run() -> dict:
    v26 = latest_v26()
    results, passed = [], 0

    cols = list(RECORD_MAX) + ["position"]
    df = pq.read_table(v26, columns=cols).to_pandas()
    off = df[df.position.isin(["QB", "RB", "WR", "TE", "K"])]
    for stat, rec in RECORD_MAX.items():
        mx = off[stat].max()
        ok = pd.isna(mx) or mx <= rec
        results.append(("RECORD_MAX", stat, f"max={mx:.0f} <= {rec}", ok))
        passed += ok

    full = pq.read_table(
        v26,
        columns=[
            "player",
            "year",
            "season_type",
            "position",
            "rushing_yards",
            "passing_yards",
            "receiving_yards",
            "receptions",
            "rushing_tds",
            "passing_tds",
            "receiving_tds",
        ],
    ).to_pandas()
    regular = full[full.season_type == "REG"]
    for name, year, stat, known in SEASON_TOTAL:
        sub = regular[
            (regular.player.str.contains(name, na=False))
            & (regular.year == year)
            & (regular.position.isin(["QB", "RB", "WR", "TE"]))
        ]
        got = sub[stat].sum()
        ok = abs(got - known) < 0.5
        results.append(("SEASON_TOTAL", f"{name} {year} {stat}", f"{got:.0f} == {known}", ok))
        passed += ok

    for name, year, stat, known in PRE1978_TOTAL:
        sub = full[(full.player.str.contains(name, na=False)) & (full.year == year)]
        got = sub[stat].sum()
        ok = abs(got - known) < 3
        results.append(("PRE1978_TOTAL", f"{name} {year} {stat}", f"{got:.0f} == {known}", ok))
        passed += ok

    for name, stat, known in CAREER_TOTAL:
        got = regular[(regular.player == name) & (regular.position.isin(["QB", "RB", "WR", "TE"]))][stat].sum()
        ok = abs(got - known) < 1.0
        results.append(("CAREER_TOTAL", f"{name} {stat}", f"{got:.0f} == {known}", ok))
        passed += ok

    ds = pq.read_table(v26, columns=["player", "year", "season_type", "def_sacks", "def_interceptions"]).to_pandas()
    for name, year, stat, known in DEF_SEASON:
        got = ds[(ds.player.str.contains(name, na=False)) & (ds.year == year)][stat].sum()
        ok = abs(got - known) < 1.0
        results.append(("DEF_SEASON", f"{name} {year} {stat}", f"{got:.1f} == {known}", ok))
        passed += ok

    dsr = ds[ds.season_type == "REG"]
    for name, year, stat, known in DEF_SEASON_REG:
        got = dsr[(dsr.player == name) & (dsr.year == year)][stat].sum()
        ok = abs(got - known) < 0.6
        results.append(("DEF_SEASON_REG", f"{name} {year} {stat}", f"{got:.1f} == {known}", ok))
        passed += ok

    ks = pq.read_table(v26, columns=["player", "year", "season_type", "fg_made"]).to_pandas()
    ks = ks[ks.season_type == "REG"]
    for name, year, stat, known in KICKER_SEASON:
        got = ks[(ks.player == name) & (ks.year == year)][stat].sum()
        ok = abs(got - known) < 0.6
        results.append(("KICKER_SEASON", f"{name} {year} {stat}", f"{got:.0f} == {known}", ok))
        passed += ok

    tp = pq.read_table(
        v26,
        columns=["year", "passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"],
    ).to_pandas()
    pre94 = (
        tp[tp.year < 1994][["passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"]]
        .fillna(0)
        .to_numpy()
        .sum()
    )
    ok = pre94 == 0
    results.append(("PRESENCE", "two_point == 0 before 1994", f"sum={pre94:.0f}", ok))
    passed += ok

    return {
        "total": len(results),
        "passed": int(passed),
        "failed": len(results) - int(passed),
        "results": results,
    }


if __name__ == "__main__":
    r = run()
    for kind, name, detail, ok in r["results"]:
        print(f"  [{'PASS' if ok else 'FAIL'}] {kind:<13} {name:<34} {detail}")
    print(f"\n{r['passed']}/{r['total']} golden samples PASS" + (f"  ({r['failed']} FAIL)" if r["failed"] else ""))
