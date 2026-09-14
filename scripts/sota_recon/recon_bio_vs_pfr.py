"""
sota_recon/recon_bio_vs_pfr.py -- standing recon: is player_bio a superset of PFR's
player universe? Runs independent of any newspaper/extraction work.

The newspaper pilot surfaced 184 players PFR knows and bio doesn't; that was a symptom of
a standing gap that should be tracked continuously, not discovered by accident. This lane
reports, every run:

  FORWARD gap   PFR player_index pfr_ids with no bio row, era-bucketed, split into
                MINTABLE (has appearance witness in PFR game-grain data) vs index_only_ghost
  REVERSE drift bio.pfr_id values not in the current PFR index (ID renumber / stale snapshot;
                a source of twins)

Join key is bio.pfr_id == player_index.pfr_id (NOT bio.NFL_player_id, which is a superset
scheme -- joining on it undercounts the match by ~85%).

Appearance witness = the pfr_id appears in PFR games_played, any boxscore player table
(player_offense/defense, home/vis_starters via player_link_ids), or the boxscore scoring
table (description_link_ids). Index-only entries with no such witness are ghosts and are
NOT mint candidates.

Outputs: derived/validation/sota_recon_master/bio_vs_pfr/
  BIO_VS_PFR_RECON_SUMMARY.json + forward_gap_mintable.csv + forward_gap_ghosts.csv +
  reverse_drift.csv

    python -m scripts.sota_recon.recon_bio_vs_pfr
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb

from .sources import DATA_LAKE, PFR_PLAYER_INDEX, PLAYER_BIO

OUT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "bio_vs_pfr"
GAMES_PLAYED = os.path.join(DATA_LAKE, "raw", "pfr", "players", "tables",
                            "games_played", "_combined.parquet").replace("\\", "/")
SCORING = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables",
                       "scoring", "_combined.parquet").replace("\\", "/")
BOX_TABLES = ["player_offense", "player_defense", "home_starters", "vis_starters"]


def _box_path(t: str) -> str:
    return os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables", t,
                        "_combined.parquet").replace("\\", "/")


def _era_case(col: str) -> str:
    return (f"CASE WHEN {col} IS NULL THEN 'null' WHEN {col}<1940 THEN '<1940' "
            f"WHEN {col}<1960 THEN '1940-59' WHEN {col}<1980 THEN '1960-79' "
            f"WHEN {col}<2000 THEN '1980-99' WHEN {col}<2010 THEN '2000-09' "
            f"ELSE '2010+' END")


def build_views(con) -> None:
    pi = Path(PFR_PLAYER_INDEX.path).as_posix()
    bio = Path(PLAYER_BIO.path).as_posix()
    con.execute(f"CREATE VIEW pi AS SELECT * FROM read_parquet('{pi}')")
    con.execute(f"CREATE VIEW bio AS SELECT * FROM read_parquet('{bio}')")
    box_union = " UNION ALL ".join(
        f"SELECT unnest(string_split(player_link_ids,';')) id FROM read_parquet('{_box_path(t)}') "
        f"WHERE player_link_ids IS NOT NULL AND player_link_ids<>''" for t in BOX_TABLES)
    con.execute(f"""
        CREATE TEMP TABLE appear AS
        SELECT DISTINCT id FROM (
            SELECT pfr_id id FROM read_parquet('{GAMES_PLAYED}') WHERE pfr_id IS NOT NULL
            UNION ALL {box_union}
            UNION ALL SELECT unnest(string_split(description_link_ids,';'))
                      FROM read_parquet('{SCORING}')
                      WHERE description_link_ids IS NOT NULL AND description_link_ids<>''
        ) WHERE id IS NOT NULL AND id<>''""")
    # A PFR pfr_id can relate to bio three ways once it is absent from bio.pfr_id:
    #   mintable  -- absent from bio entirely (no pfr_id, no NFL_player_id) -> new row
    #   relink    -- present as bio.NFL_player_id with bio.pfr_id NULL -> same player, link only
    #   twin      -- present as bio.NFL_player_id but bio.pfr_id is a DIFFERENT id -> conflict
    con.execute("""
        CREATE TEMP TABLE gap AS
        SELECT p.pfr_id, p.player, TRY_CAST(p.first_year AS INT) first_year,
               TRY_CAST(p.last_year AS INT) last_year, p.index_position,
               (a.id IS NOT NULL) AS has_appearance,
               bn.NFL_player_id AS bio_nflid, bn.pfr_id AS bio_existing_pfrid,
               CASE WHEN bn.NFL_player_id IS NULL THEN 'mintable'
                    WHEN bn.pfr_id IS NULL THEN 'relink'
                    ELSE 'twin' END AS klass
        FROM pi p
        LEFT JOIN (SELECT DISTINCT pfr_id FROM bio WHERE pfr_id IS NOT NULL) b USING (pfr_id)
        LEFT JOIN bio bn ON bn.NFL_player_id = p.pfr_id
        LEFT JOIN appear a ON a.id = p.pfr_id
        WHERE b.pfr_id IS NULL AND p.pfr_id IS NOT NULL""")


def run() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    build_views(con)

    era = _era_case("first_year")
    # mintable = truly absent AND appearance-witnessed; relink/twin are separate lanes
    forward_by_era = {r[0]: {"mintable": r[1], "ghost": r[2]} for r in con.execute(f"""
        SELECT {era} e, COUNT(*) FILTER (WHERE has_appearance AND klass='mintable') mint,
               COUNT(*) FILTER (WHERE NOT has_appearance AND klass='mintable') ghost
        FROM gap GROUP BY 1 ORDER BY 1""").fetchall()}
    mint_n, ghost_n, relink_n, twin_n = con.execute("""
        SELECT COUNT(*) FILTER (WHERE has_appearance AND klass='mintable'),
               COUNT(*) FILTER (WHERE NOT has_appearance AND klass='mintable'),
               COUNT(*) FILTER (WHERE klass='relink'),
               COUNT(*) FILTER (WHERE klass='twin') FROM gap""").fetchone()

    con.execute(f"""COPY (SELECT pfr_id, player, first_year, last_year, index_position
        FROM gap WHERE has_appearance AND klass='mintable' ORDER BY first_year, pfr_id)
        TO '{(OUT/'forward_gap_mintable.csv').as_posix()}' (HEADER)""")
    con.execute(f"""COPY (SELECT pfr_id, player, first_year, last_year, index_position
        FROM gap WHERE NOT has_appearance AND klass='mintable' ORDER BY first_year, pfr_id)
        TO '{(OUT/'forward_gap_ghosts.csv').as_posix()}' (HEADER)""")
    con.execute(f"""COPY (SELECT pfr_id, player, bio_nflid, first_year, last_year
        FROM gap WHERE klass='relink' ORDER BY pfr_id)
        TO '{(OUT/'forward_gap_relink.csv').as_posix()}' (HEADER)""")
    con.execute(f"""COPY (SELECT pfr_id, player, bio_nflid, bio_existing_pfrid, first_year, last_year
        FROM gap WHERE klass='twin' ORDER BY pfr_id)
        TO '{(OUT/'forward_gap_twin_review.csv').as_posix()}' (HEADER)""")

    drift_n = con.execute("""
        SELECT COUNT(*) FROM (SELECT DISTINCT pfr_id FROM bio WHERE pfr_id IS NOT NULL) b
        LEFT JOIN (SELECT DISTINCT pfr_id FROM pi) p USING (pfr_id) WHERE p.pfr_id IS NULL
    """).fetchone()[0]
    con.execute(f"""COPY (
        SELECT b.pfr_id, b.player, b.first_year, b.last_year FROM bio b
        LEFT JOIN (SELECT DISTINCT pfr_id FROM pi) p USING (pfr_id)
        WHERE p.pfr_id IS NULL AND b.pfr_id IS NOT NULL ORDER BY b.pfr_id)
        TO '{(OUT/'reverse_drift.csv').as_posix()}' (HEADER)""")

    pi_n = con.execute("SELECT COUNT(DISTINCT pfr_id) FROM pi").fetchone()[0]
    bio_n = con.execute("SELECT COUNT(DISTINCT pfr_id) FROM bio WHERE pfr_id IS NOT NULL").fetchone()[0]
    con.close()

    summary = {
        "lane": "bio_vs_pfr",
        "pfr_index_players": pi_n,
        "bio_players_with_pfr_id": bio_n,
        "forward_gap_total": mint_n + ghost_n + relink_n + twin_n,
        "forward_gap_mintable": mint_n,
        "forward_gap_index_only_ghost": ghost_n,
        "forward_gap_relink_existing_row": relink_n,
        "forward_gap_twin_review": twin_n,
        "forward_gap_by_era": forward_by_era,
        "reverse_drift_bio_ids_not_in_pfr": drift_n,
        "join_key": "bio.pfr_id == player_index.pfr_id",
        "appearance_witness": "games_played | boxscore player_link_ids | scoring description_link_ids",
        "status": "review" if (mint_n or drift_n) else "pass",
        "cloud_write_performed": False,
        "destructive_actions_performed": False,
        "write_guarantee": "Recon artifacts only under sota_recon_master/bio_vs_pfr; no table writes.",
    }
    with open(OUT / "BIO_VS_PFR_RECON_SUMMARY.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
