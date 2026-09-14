"""StatsCrew team-code canonicalization (Joe, 2026-08-02): StatsCrew keys teams
in its own code space ('CAN', 'GB', 'SD'). The results caption carries the full
team name per season ('1920 Canton Bulldogs Game-by-Game Results'), whose
nickname routes through the self-built nickname crosswalk -- the same lane the
scoring log used. Output: (sc_code, season, team_code, confidence) map.

Ambiguity discipline: a (sc_code, season) with more than one candidate catalog
code is emitted with each candidate's share; consumers gate on confidence
>= 0.95 exactly like the nickname crosswalk itself.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "statscrew_team_code_map.parquet"
RECEIPT = LAKE / "statscrew_team_code_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    src = Path(S.registry()["statscrew_team_season_results"].path)
    p = src.as_posix() + ("/**/*.parquet" if src.is_dir() else "")
    nick = (LAKE / "nickname_team_code_crosswalk.parquet").as_posix()

    # sc code = first token of `team` (rows sometimes carry 'CAN CL1' pair
    # artifacts; the page's own team is always the first token). Nickname =
    # last word of the caption's team name.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE pages AS
    SELECT str_split(team, ' ')[1] AS sc_code,
           TRY_CAST(season AS INT) AS season,
           LOWER(regexp_extract(table_caption,
                 '^\\d{{4}} (.*) Game-by-Game', 1)) AS team_name,
           COUNT(*) AS n_rows
    FROM read_parquet('{p}', union_by_name=true)
    WHERE team IS NOT NULL AND table_caption IS NOT NULL
    GROUP BY 1, 2, 3""")

    con.execute(f"""
    COPY (
      WITH cand AS (
        SELECT pg.sc_code, pg.season, n.team_code, SUM(pg.n_rows) AS w
        FROM pages pg
        JOIN read_parquet('{nick}') n
          ON n.season = pg.season AND n.confidence >= 0.95
         AND LOWER(n.nickname) = regexp_extract(pg.team_name, '(\\S+)$', 1)
        GROUP BY 1, 2, 3)
      SELECT sc_code, season, team_code,
             w / SUM(w) OVER (PARTITION BY sc_code, season) AS confidence
      FROM cand
    ) TO '{OUT.as_posix()}' (FORMAT parquet)""")

    stats = con.execute(f"""
    SELECT COUNT(*), COUNT(*) FILTER (WHERE confidence >= 0.95),
           COUNT(DISTINCT (sc_code, season))
    FROM read_parquet('{OUT.as_posix()}')""").fetchone()
    covered = con.execute(f"""
    SELECT COUNT(DISTINCT (sc_code, season)) FROM pages""").fetchone()[0]
    receipt = {
        "wave": "statscrew_team_codes", "date": "2026-08-02",
        "map_rows": stats[0], "confident_rows": stats[1],
        "code_seasons_mapped": stats[2], "code_seasons_total": covered,
        "coverage": round(stats[2] / max(covered, 1), 4),
        "route": "caption team-name nickname -> nickname_team_code_crosswalk",
        "out": str(OUT),
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2))
