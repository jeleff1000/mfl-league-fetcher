"""Measure a witness on the POPULATION IT ACTUALLY COVERS.

WHY. A source table's name often encodes a scope our subject does not have. PFR's defense
table and NFL.com's Defense page carry DEFENSIVE PLAYERS; our `def_tackles_solo` credits
whoever the play-by-play names, including an offensive lineman who tackles a defender after
an interception. Every tackle measurement this programme made compared a scoped source
against an unscoped subject, and it cost ~10 points of apparent agreement:

    vs PFR season   2010-24  solo 90.44% -> 96.57%      2000-09  84.90% -> 98.81%
    vs nflcom       2005-19  tkl  76.53% -> 89.80%      2010+    86.3%  -> 97.2%

RECEIPT that the scope is the cause and not a convenient filter: of the 1,448 player-games
where we credited a tackle and PFR had no line at all, 92.9% are offensive linemen (G 515 /
OT 495 / C 335) against a population where tackle-bearing weeks run DB 49,675 / LB 35,657 /
DL 33,578 and G+OT total about 1,500. The linemen are over-represented in the disputed set
and nowhere else.

SCOPING IS A DECISION, NOT A FILTER. Narrowing a comparison until it agrees is how an audit
lies to itself, so every population here declares:
  * WHY the source is scoped (what the page or table is)
  * the CONTROL that shows the scope is the cause
  * the columns it applies to, named -- never "the defensive ones, roughly"
and the unscoped number is kept beside the scoped one so the move is always visible.

    python -m scripts.sota_recon.nflcom_population_scoped_audit
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from . import sources as S

LEDGER = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
          r"\nflcom_population_scoped_receipts.json")

#: A population is a claim about what a witness covers, with the evidence for it.
POPULATIONS = {
    "defensive_players": {
        "positions": ("DB", "LB", "DL"),
        "why": "NFL.com's Defense Career page and PFR's player-defense table are DEFENSIVE "
               "surfaces: they carry defensive players. v26 def_tackles_* credits whoever "
               "the play-by-play names, which includes offensive players tackling after a "
               "turnover.",
        "control": "Of the 1,448 player-games we credit and PFR has no line for, 92.9% are "
                   "offensive linemen (G 515 / OT 495 / C 335, only 20 defenders), against "
                   "a population where tackle-bearing weeks are DB 49,675 / LB 35,657 / "
                   "DL 33,578 and G+OT total ~1,500.",
    },
}

#: (source, physical table, source column, canonical, population). Named one at a time --
#: a rule like "every defensive column" would silently scope the next column added.
SCOPED = [
    ("nflcom_player_career", "Defense Career", "tkl", "def_tackles_solo",
     "defensive_players"),
    ("nflcom_player_career", "Defense Career", "combined", "def_tackles_combined",
     "defensive_players"),
    ("nflcom_player_career", "Defense Career", "ast", "def_tackle_assists",
     "defensive_players"),
]

#: The licence stratum, so a scoped floor is comparable to an unscoped licence.
YEARS = (2005, 2019)


def build() -> dict:
    xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()
    nfc = "D:/league-history-data/nfl/raw/nflcom/tables/player_career/**/*.parquet"
    con = duckdb.connect(); con.execute("SET memory_limit='6GB'")
    con.execute(f"""CREATE TABLE V AS SELECT x.nflcom_slug slug, b.nfl_position pos, t.*
        FROM (SELECT NFL_player_id nid, year yr, SUM(def_tackles_solo) def_tackles_solo,
                     SUM(def_tackle_assists) def_tackle_assists,
                     SUM(def_tackles_combined) def_tackles_combined
              FROM read_parquet('{v26}') WHERE season_type='REG' GROUP BY 1,2) t
        JOIN read_parquet('{bio}') b ON b.NFL_player_id = t.nid
        JOIN read_parquet('{xw}') x ON x.pfr_id = b.pfr_id""")
    con.execute(f"""CREATE TABLE C AS SELECT DISTINCT * FROM
        read_parquet('{nfc}', union_by_name=True)""")
    rows = []
    for source, table, col, canon, popname in SCOPED:
        pop = POPULATIONS[popname]
        inlist = ", ".join(f"'{p}'" for p in pop["positions"])
        base = f"""
          WITH a AS (SELECT nflcom_slug slug, TRY_CAST(season AS INT) yr,
                            SUM(TRY_CAST("{col}" AS DOUBLE)) v
                     FROM C WHERE _table='{table}'
                       AND TRY_CAST("{col}" AS DOUBLE) IS NOT NULL GROUP BY 1,2)
          SELECT COUNT(*) n,
                 ROUND(100.0*COUNT(*) FILTER (WHERE a.v = V.{canon})/NULLIF(COUNT(*),0), 4)
          FROM a JOIN V USING (slug, yr)
          WHERE yr BETWEEN {YEARS[0]} AND {YEARS[1]}"""
        n_all, pct_all = con.execute(base).fetchone()
        n_sc, pct_sc = con.execute(base + f" AND V.pos IN ({inlist})").fetchone()
        rows.append({
            "key": f"{source}|{table}|{col}|{canon}|{popname}",
            "source": source, "source_table": table, "column": col, "canonical": canon,
            "population": popname, "stratum": f"{YEARS[0]}-{YEARS[1]}",
            "n_scoped": n_sc, "agree_pct": round((pct_sc or 0) / 100, 4),
            "n_unscoped": n_all, "agree_pct_unscoped": round((pct_all or 0) / 100, 4),
        })
    con.close()
    return {"populations": POPULATIONS, "rows": rows}


def main() -> int:
    rep = build()
    print(f"{'column':10s} {'canonical':24s} {'scoped':>9s} {'unscoped':>9s} {'n':>8s}")
    for r in rep["rows"]:
        print(f"{r['column']:10s} {r['canonical']:24s} {r['agree_pct']:>9.4f} "
              f"{r['agree_pct_unscoped']:>9.4f} {r['n_scoped']:>8,}")
    Path(LEDGER).write_text(json.dumps(rep, indent=1, default=str), encoding="utf-8")
    print(f"\nreceipts -> {LEDGER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
