"""CATALOG-IDENTITY CLASS CLOSURE: the game-context columns close against
the authoritative game catalog (nfl_team_games_all -- team record source,
per standing law).

For each identity/context column with a catalog counterpart, measure
per-row identity between the plane and the catalog joined on
(team, year, week): team codes, opponent codes, dates, home/away,
franchise numbers, scores, win flags. A column at >= 0.999 identity closes
with the catalog as its witness (root: catalog). Mismatches are listed as
work items -- never forced.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
RECEIPT = LAKE / "catalog_identity_receipt.json"

# plane column -> (catalog expr over c.*, comparison)
CHECKS = {
    "opponent_nfl_team": ("c.opponent_code", "text"),
    # plane stores a timestamp; compare at DATE precision (0.0000 was a
    # format artifact, caught 2026-08-04)
    "game_date": ("CAST(TRY_CAST(c.game_date AS DATE) AS VARCHAR)",
                  "date"),
    "home_away": ("CASE WHEN c.is_home = 1 THEN 'home' ELSE 'away' END",
                  "text_lower"),
    "team_points": ("TRY_CAST(c.team_points AS DOUBLE)", "num"),
    "opponent_points": ("TRY_CAST(c.opponent_points AS DOUBLE)", "num"),
    # ties code as 0 in the plane (a tie is not a win) -- 10,519 tie rows
    # measured; the record-book convention stands
    "is_win": ("CASE WHEN c.result = 'W' THEN 1.0 ELSE 0.0 END", "num"),
    "game_margin": ("TRY_CAST(c.team_points AS DOUBLE) "
                    "- TRY_CAST(c.opponent_points AS DOUBLE)", "num"),
    "nfl_franchise_number": ("TRY_CAST(c.team_fid AS DOUBLE)", "num"),
    "opponent_nfl_franchise_number": ("TRY_CAST(c2.team_fid AS DOUBLE)",
                                      "num_opp"),
}


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW cat AS
    SELECT * FROM '{games}' WHERE season_type = 'REG'""")
    out = {}
    for col, (expr, kind) in CHECKS.items():
        opp_join = ("""JOIN cat c2 ON c2.team_code = t.opponent_nfl_team
            AND CAST(c2.year AS INT) = CAST(t.year AS INT)
            AND TRY_CAST(c2.week AS INT) = TRY_CAST(t.week AS INT)"""
                    if kind == "num_opp" else "")
        if kind == "date":
            cmp_ = (f"CAST(TRY_CAST(t.{col} AS DATE) AS VARCHAR) = ({expr})")
        elif kind == "text":
            cmp_ = f"LOWER(TRIM(CAST(t.{col} AS VARCHAR))) = LOWER(TRIM({expr}))"
        elif kind == "text_lower":
            cmp_ = f"LOWER(TRIM(CAST(t.{col} AS VARCHAR))) = LOWER(TRIM({expr}))"
        else:
            cmp_ = f"ABS(TRY_CAST(t.{col} AS DOUBLE) - ({expr})) <= 0.001"
        try:
            n, ok = con.execute(f"""
            SELECT COUNT(*), COUNT(*) FILTER (WHERE {cmp_})
            FROM read_parquet('{wk}') t
            JOIN cat c ON c.team_code = t.nfl_team
              AND CAST(c.year AS INT) = CAST(t.year AS INT)
              AND TRY_CAST(c.week AS INT) = TRY_CAST(t.week AS INT)
            {opp_join}
            WHERE t.season_type = 'REG' AND t.{col} IS NOT NULL""").fetchone()
        except Exception as e:
            out[col] = {"error": str(e).splitlines()[0][:70]}
            continue
        out[col] = {"n": n, "identity": round(ok / n, 5) if n else None,
                    "closes": bool(n and ok / n >= 0.999)}
        print(f"{col:34s} n={n:8d} identity={ok/n:.5f} "
              f"{'CLOSES' if out[col]['closes'] else 'work item'}", flush=True)
    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"),
         "witness": "nfl_team_games_all (authoritative catalog)",
         "results": out}, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
