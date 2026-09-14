"""THE SEASON POSITION DECLARATION, from PFR's own per-season Pos field.

Joe 2026-08-05: "Its not if PFR carries a line for it with our positons. Its
if PFR SAYS Travis Hunter is WR,CB in his actual bio for that season" and
"You see how Deion has that one official WR year? thats how we declare dual
players".

Deion Sanders' PFR page reads RCB every year, LCB in 1995, and RCB/WR in
1996 -- ONE official WR year. That slash is the declaration. Dual
eligibility is what PFR SAYS on the season row, never what we infer from a
player having a stat line in some table.

WHY THE PREVIOUS SOURCE WAS WRONG. v26's season_positions was built from
STAT-LINE PRESENCE: appear in the kicking table and you are declared a
kicker. That made Frank Gifford RB,K in 1956 on the strength of ONE field
goal attempt -- and PFR's own pos for that season reads LH, even inside the
kicking table itself. Presence in a table is not a claim about position.

SAME-BROAD SLASHES ARE ALIGNMENT, NOT DUALITY. LCB/RCB, RDE/LDE, FS/SS and
LT/RT are which side of the formation a man lined up on. Only a slash that
crosses BROAD positions (RCB/WR) is dual eligibility, which is why the
tokens are mapped through the taxonomy and de-duplicated before counting.

Output: one row per (NFL_player_id, year) with the declared broad set.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "pfr_season_position_declaration.parquet"
TAX = Path(__file__).parent / "witness_gate" / "contracts" / "position_taxonomy.v1.json"


def lanes(con) -> list[str]:
    """Every PFR lane carrying (pfr_id, year_id, pos)."""
    out = []
    for name in sorted(n for n in dir(S) if n.isupper() and n.startswith("PFR_")):
        src = getattr(S, name)
        path = getattr(src, "path", None)
        if not path:
            continue
        try:
            cols = {c[0] for c in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 0").fetchall()}
        except Exception:
            continue
        if {"pfr_id", "pos", "year_id"} <= cols:
            out.append(f"SELECT pfr_id, TRY_CAST(year_id AS INT) AS yr, pos "
                       f"FROM read_parquet('{path}') "
                       f"WHERE pos IS NOT NULL AND pos <> ''")
    return out


def build(con) -> dict:
    d2b = json.loads(TAX.read_text(encoding="utf-8"))["detailed_to_broad"]
    whens = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in d2b.items())
    parts = lanes(con)
    if not parts:
        raise SystemExit("no PFR lane exposes (pfr_id, year_id, pos)")
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE decl AS
    WITH raw AS ({' UNION ALL '.join(parts)}),
    tok AS (SELECT pfr_id, yr,
                   UNNEST(str_split_regex(UPPER(TRIM(pos)), '[/,]')) AS t
            FROM raw WHERE yr IS NOT NULL),
    mapped AS (SELECT pfr_id, yr,
                      list_sort(list_distinct(list_filter(
                        list(CASE TRIM(t) {whens} END), x -> x IS NOT NULL))) AS broads
               FROM tok GROUP BY 1, 2)
    SELECT b.NFL_player_id, m.yr AS year, m.broads,
           array_to_string(m.broads, ',') AS declared
    FROM mapped m
    JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{bio}')
          WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL) b
      ON b.pfr_id = m.pfr_id
    WHERE len(m.broads) > 0""")
    n, dual, players = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE len(broads) > 1), "
        "COUNT(DISTINCT NFL_player_id) FROM decl").fetchone()
    con.execute(f"COPY (SELECT * FROM decl) TO '{OUT.as_posix()}' "
                f"(FORMAT parquet, ROW_GROUP_SIZE 20000)")
    return {"lanes": len(parts), "player_seasons": n, "dual_seasons": dual,
            "players": players, "out": str(OUT)}


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='1400MB'")
    con.execute("SET threads=2")
    print(json.dumps(build(con), indent=2))
