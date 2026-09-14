"""
sota_recon/build_newspaper_identity_resolution_v26.py -- resolve newspaper identity-held
atoms against the EXPANDED bio, gated by PFR per-game appearance (twin-safe).

The 1,118 identity holds were atoms whose raw newspaper name never resolved to an
NFL_player_id. Most were held only because the player wasn't in bio -- which the wave59
backfill just fixed (+2,873 players). This pass re-resolves them, but never by name alone:

  resolve  IFF a unique bio player matches (raw last name + year within career span) AND
           PFR independently places that exact pfr_id in THIS boxscore (scoring
           description_link_ids / starters / offense / defense player_link_ids).
           PFR-in-this-game is the twin breaker: two players share a surname, but only the
           one who actually played this game appears in its box.
  disambiguate  when several bio candidates share the surname, PFR appearance in THIS game
                picks the one who played (resolve iff exactly one candidate appears).
  hold     unique-by-name but PFR has no appearance record for the game (uncorroborated),
           or zero / multiple appearing candidates.

Output: a resolution LEDGER (not applied here) consumed by the payload assembly + upsert:
  derived/validation/sota_recon_master/newspaper_identity/RESOLUTION_LEDGER.csv + summary.

    python -m scripts.sota_recon.build_newspaper_identity_resolution_v26
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb

from .sources import DATA_LAKE, PLAYER_BIO

OUT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "newspaper_identity"
HOLDS = (Path(DATA_LAKE) / "curated" / "witnesses" / "newspaper" / "20260717T065218Z_v1"
         / "holds" / "remaining_identity_hardhold_queue.csv").as_posix()
BOX = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables")


def _appearance_union() -> str:
    parts = []
    for t, col in [("scoring", "description_link_ids"), ("home_starters", "player_link_ids"),
                   ("vis_starters", "player_link_ids"), ("player_offense", "player_link_ids"),
                   ("player_defense", "player_link_ids")]:
        p = os.path.join(BOX, t, "_combined.parquet").replace("\\", "/")
        parts.append(f"SELECT boxscore_id, unnest(string_split({col}, ';')) pid "
                     f"FROM read_parquet('{p}') WHERE {col} IS NOT NULL AND {col} <> ''")
    return " UNION ALL ".join(parts)


def run() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    bio = Path(PLAYER_BIO.path).as_posix()

    con.execute(f"""CREATE TEMP TABLE hold AS
        SELECT identity_task_id, boxscore_id, TRY_CAST(year AS INT) yr, nfl_team, role,
               raw_player,
               lower(regexp_extract(trim(raw_player), '([A-Za-z]+)$', 1)) lname
        FROM read_csv_auto('{HOLDS}')
        WHERE raw_player IS NOT NULL AND raw_player NOT ILIKE '% and %'
          AND length(regexp_replace(raw_player, '[^A-Za-z]', '')) >= 3
          AND year IS NOT NULL AND boxscore_id IS NOT NULL""")
    con.execute(f"""CREATE TEMP TABLE bp AS
        SELECT pfr_id, player,
               lower(regexp_extract(trim(player), '([A-Za-z]+)$', 1)) lname,
               TRY_CAST(first_year AS INT) fy, TRY_CAST(last_year AS INT) ly
        FROM read_parquet('{bio}') WHERE pfr_id IS NOT NULL""")
    con.execute(f"CREATE TEMP TABLE appear AS SELECT DISTINCT boxscore_id, pid "
                f"FROM ({_appearance_union()}) WHERE pid IS NOT NULL AND pid <> ''")

    # candidates per held atom, flagged by whether PFR places them in THIS game
    con.execute("""CREATE TEMP TABLE cand AS
        SELECT h.identity_task_id, h.boxscore_id, h.yr, h.nfl_team, h.raw_player,
               b.pfr_id, b.player,
               (a.pid IS NOT NULL) AS appears_in_game
        FROM hold h
        JOIN bp b ON b.lname = h.lname AND h.yr BETWEEN b.fy AND b.ly
        LEFT JOIN appear a ON a.boxscore_id = h.boxscore_id AND a.pid = b.pfr_id""")
    con.execute("""CREATE TEMP TABLE resolved AS
        WITH agg AS (
            SELECT identity_task_id, boxscore_id, yr, nfl_team, raw_player,
                   COUNT(*) n_cand,
                   COUNT(*) FILTER (WHERE appears_in_game) n_appear,
                   MAX(CASE WHEN appears_in_game THEN pfr_id END) appear_pid,
                   MAX(CASE WHEN appears_in_game THEN player END) appear_name,
                   MAX(pfr_id) sole_pid, MAX(player) sole_name
            FROM cand GROUP BY 1,2,3,4,5)
        SELECT *,
            CASE WHEN n_appear = 1 THEN 'resolved_pfr_corroborated'
                 WHEN n_cand = 1 AND n_appear = 0 THEN 'hold_unique_but_uncorroborated'
                 WHEN n_appear > 1 THEN 'hold_multiple_appear'
                 ELSE 'hold_ambiguous_no_appearance' END verdict,
            CASE WHEN n_appear = 1 THEN appear_pid END resolved_nfl_id,
            CASE WHEN n_appear = 1 THEN appear_name END resolved_player
        FROM agg""")

    con.execute(f"""COPY (SELECT identity_task_id, boxscore_id, yr, nfl_team, raw_player,
        verdict, resolved_nfl_id, resolved_player, n_cand, n_appear
        FROM resolved ORDER BY verdict, boxscore_id)
        TO '{(OUT/'RESOLUTION_LEDGER.csv').as_posix()}' (HEADER)""")

    counts = dict(con.execute("SELECT verdict, COUNT(*) FROM resolved GROUP BY 1 ORDER BY 2 DESC").fetchall())
    n_hold_total = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{HOLDS}')").fetchone()[0]
    con.close()
    summary = {
        "lane": "newspaper_identity_resolution",
        "holds_total": n_hold_total,
        "verdicts": counts,
        "resolved": counts.get("resolved_pfr_corroborated", 0),
        "gate": "unique bio surname+career-span match AND PFR appearance in the same boxscore (twin-safe)",
        "ledger": str(OUT / "RESOLUTION_LEDGER.csv"),
        "write_guarantee": "resolution ledger only; no table writes",
    }
    with open(OUT / "IDENTITY_RESOLUTION_SUMMARY.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
