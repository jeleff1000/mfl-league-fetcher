"""
sota_recon/recon_position_era.py  --  every position x every micro-era cross-section

"Golden samples for every position in every micro-era." NFL statistics are not uniform
across history: what is recorded, and the record values, shift at rule/recording
boundaries. This lane makes the full grid EXPLICIT -- for each (position group, micro-era,
stat) it reports v26's leader (value + who + year), so every cell is visible and anomalies
(an impossible max, or zero where data should exist) jump out.

CRITICAL: IDP (individual defensive players: DB/DL/LB...) and DST (the team DEF row) are
SEPARATE levels and are reported separately -- never summed together (the team DEF row
already equals the sum of its IDP rows; mixing them double-counts).

Micro-eras (statistical, not merely decade):
  1920-1931 scoring-only | 1932-1945 early-box | 1946-1949 AAFC | 1950-1959 |
  1960-1969 AFL+NFL | 1970-1977 merged pre-PBP | 1978-1981 PBP-begins |
  1982-1993 official-sacks | 1994-2001 two-point | 2002-2020 modern | 2021-2025 17-game

    python -m scripts.sota_recon.recon_position_era
      -> derived/validation/expectations/POSITION_ERA_MATRIX.md
"""

from __future__ import annotations

import os

import duckdb

from .sources import latest_v26

OUT = "D:/league-history-data/nfl/derived/validation/expectations"

ERAS = [(1920, 1931), (1932, 1945), (1946, 1949), (1950, 1959), (1960, 1969),
        (1970, 1977), (1978, 1981), (1982, 1993), (1994, 2001), (2002, 2020), (2021, 2025)]

IDP = "('DB','DL','LB','DE','DT','CB','S','NT','EDGE','ILB','OLB','MLB','FS','SS')"

# group -> (position filter SQL, [stats]). DST is the team DEF row; IDP the individuals.
GROUPS = {
    "QB":  ("position='QB'", ["passing_yards", "passing_tds", "passing_interceptions", "completions"]),
    "RB":  ("position IN ('RB','FB','HB')", ["rushing_yards", "rushing_tds", "carries"]),
    "WR":  ("position='WR'", ["receiving_yards", "receiving_tds", "receptions"]),
    "TE":  ("position='TE'", ["receiving_yards", "receiving_tds", "receptions"]),
    "K":   ("position='K'", ["fg_made", "pat_made"]),
    "P":   ("position='P'", ["punts", "punt_yards"]),
    "IDP": (f"position IN {IDP}", ["def_interceptions", "def_sacks", "def_tackles_solo", "def_tds"]),
    "DST": ("position='DEF'", ["def_interceptions", "def_sacks", "def_safeties", "special_teams_tds"]),
}


def run() -> dict:
    v26 = latest_v26()
    have = set(duckdb.connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{v26}')").df()["column_name"])
    con = duckdb.connect(); con.execute("SET memory_limit='3GB'")
    V = f"read_parquet('{v26}')"
    lines = ["# Position x Micro-era cross-section (v26 single-game leaders)", "",
             "Each cell = max single-game value in v26 for that position group & era, with "
             "holder/year. IDP (individual) and DST (team DEF row) are SEPARATE. Use to spot "
             "impossible maxes or empty cells where data should exist.", ""]
    rows_out = []
    for grp, (posf, stats) in GROUPS.items():
        stats = [s for s in stats if s in have]
        lines.append(f"\n## {grp}\n")
        lines.append("| era | " + " | ".join(stats) + " |")
        lines.append("|" + "---|" * (len(stats) + 1))
        for lo, hi in ERAS:
            cells = []
            for s in stats:
                r = con.execute(f"""
                    SELECT {s} v, player, year FROM {V}
                    WHERE {posf} AND year BETWEEN {lo} AND {hi} AND {s} IS NOT NULL
                    ORDER BY {s} DESC NULLS LAST LIMIT 1
                """).fetchone()
                if r and r[0] is not None and r[0] != 0:
                    cells.append(f"{r[0]:g} ({str(r[1])[:14]} {int(r[2])})")
                    rows_out.append((grp, f"{lo}-{hi}", s, float(r[0]), r[1], int(r[2])))
                else:
                    cells.append("—")
            lines.append(f"| {lo}-{hi} | " + " | ".join(cells) + " |")
    con.close()
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "POSITION_ERA_MATRIX.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return {"cells_populated": len(rows_out), "out": os.path.join(OUT, "POSITION_ERA_MATRIX.md")}


if __name__ == "__main__":
    r = run()
    print(f"populated {r['cells_populated']} (group x era x stat) leader cells")
    print(f"wrote {r['out']}")
