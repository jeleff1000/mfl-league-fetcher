"""IS_STARTER RECEIPTED ZEROS (union guard, per Joe's design correction:
"other sources were grabbing players the PFR lacked a lot of the time").

A NULL is_starter cell becomes a receipted 0 ONLY when ALL hold:
  (a) the game's PFR starter tables are COMPLETE (11 rows/side one-platoon
      era pre-1950, 22/side after);
  (b) the player is attested in the same game's PFR box (they played);
  (c) the player is NOT listed in the starters union for that game;
  (d) NO other lineup source (newspaper_lineups pilot) lists them starting.
Any cross-source disagreement is LOGGED, never auto-zeroed.
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
OUT = LAKE / "weekly_overlay_is_starter_zeros.parquet"
RECEIPT = LAKE / "is_starter_zeros_receipt.json"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    reg = S.registry()

    def pp(key):
        q = Path(reg[key].path)
        return (q.as_posix() + "/**/*.parquet") if q.is_dir() else q.as_posix()

    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()

    con.execute(f"""CREATE OR REPLACE TEMP TABLE starters AS
    SELECT boxscore_id, player_link_ids AS pfr_id
    FROM (SELECT boxscore_id, player_link_ids
          FROM read_parquet('{pp('pfr_box_home_starters')}')
          UNION ALL SELECT boxscore_id, player_link_ids
          FROM read_parquet('{pp('pfr_box_vis_starters')}'))
    WHERE player_link_ids IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE complete_games AS
    SELECT s.boxscore_id
    FROM (SELECT boxscore_id, COUNT(*) AS n FROM starters GROUP BY 1) s
    JOIN (SELECT DISTINCT boxscore_id, CAST(year AS INT) AS yr
          FROM '{games}' WHERE season_type = 'REG') g USING (boxscore_id)
    WHERE (g.yr < 1950 AND s.n = 22) OR (g.yr >= 1950 AND s.n = 44)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE played AS
    SELECT DISTINCT boxscore_id,
           regexp_extract(CAST(player_link_ids AS VARCHAR),
                          '^([^;,]+)', 1) AS pfr_id
    FROM (SELECT boxscore_id, player_link_ids
          FROM read_parquet('{pp('pfr_player_offense_box')}')
          UNION ALL SELECT boxscore_id, player_link_ids
          FROM read_parquet('{pp('pfr_player_defense_box')}'))
    WHERE player_link_ids IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE news_starters AS
    SELECT DISTINCT boxscore_id, NFL_player_id
    FROM read_parquet('{pp('newspaper_lineups')}')
    WHERE starter_position IS NOT NULL OR
          LOWER(COALESCE(participation_type, '')) LIKE '%start%'""")

    con.execute(f"""
    COPY (
    SELECT t.NFL_player_id, t.year, t.week, 'is_starter' AS column_name,
           TRY_CAST(t.is_starter AS DOUBLE) AS old_value, 0.0 AS new_value,
           'starter_zero' AS repair_id, 'starters_union_guard' AS root,
           'complete starter table + played per box + absent from all '
           || 'lineup sources' AS ruling
    FROM read_parquet('{wk}') t
    JOIN (SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
          WHERE pfr_id IS NOT NULL) b USING (NFL_player_id)
    JOIN (SELECT DISTINCT boxscore_id, year, week, team_code FROM '{games}'
          WHERE season_type = 'REG') g
      ON g.team_code = t.nfl_team AND CAST(g.year AS INT) = CAST(t.year AS INT)
      AND TRY_CAST(g.week AS INT) = TRY_CAST(t.week AS INT)
    JOIN complete_games cg ON cg.boxscore_id = g.boxscore_id
    JOIN played pl ON pl.boxscore_id = g.boxscore_id
      AND pl.pfr_id = b.pfr_id
    ANTI JOIN starters st ON st.boxscore_id = g.boxscore_id
      AND st.pfr_id = b.pfr_id
    ANTI JOIN news_starters ns ON ns.boxscore_id = g.boxscore_id
      AND ns.NFL_player_id = t.NFL_player_id
    WHERE t.season_type = 'REG' AND t.is_starter IS NULL
    QUALIFY COUNT(*) OVER (PARTITION BY t.NFL_player_id, t.year, t.week) = 1
    ) TO '{OUT.as_posix()}' (FORMAT parquet)""")
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT.as_posix()}')"
                    ).fetchone()[0]
    by_dec = dict(con.execute(f"""SELECT (CAST(year AS INT)/10)::INT*10,
      COUNT(*) FROM read_parquet('{OUT.as_posix()}') GROUP BY 1""").fetchall())
    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"), "cells": n,
         "by_decade": {str(k): int(v) for k, v in by_dec.items()},
         "guards": ["complete starter tables (11/22 per side by era)",
                    "played per pfr box", "absent from starters union",
                    "absent from newspaper lineups"]},
        indent=1), encoding="utf-8")
    print(f"receipted zeros: {n} cells", by_dec)


if __name__ == "__main__":
    main()
