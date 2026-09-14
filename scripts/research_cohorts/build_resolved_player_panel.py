"""Rebuild player-season playoff outcomes from the local raw corpus snapshot.

The flattened player snapshot loses team identity for many rows.  This rebuild
keeps only player-league seasons with an explicit native/backfill playoff
signal, so unresolved rows are excluded from the playoff denominator.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-panel", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--year", type=int, required=True)
    args = ap.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    corp = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb")
    ops = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb")
    temp = args.out.parent / "duckdb_temp"
    temp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"memory_limit": "5000MB", "threads": 4,
                                 "preserve_insertion_order": "false",
                                 "temp_directory": str(temp)})
    con.execute(f"ATTACH '{corp.as_posix()}' AS corp (READ_ONLY)")
    con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
    pf_cols = {row[0] for row in con.execute("DESCRIBE corp.public.player_fantasy").fetchall()}
    championship_signal = (
        "COALESCE(CAST(p.is_championship AS INTEGER),0)"
        if "is_championship" in pf_cols else "0"
    )
    panel = str(args.source_panel).replace("'", "''")
    con.execute(f"""
      CREATE OR REPLACE TEMP VIEW panel_map AS
      SELECT DISTINCT db_name, year, NFL_player_id, player, teams, roster,
             scoring, pass_td, po_slots, lineup
      FROM read_parquet('{panel}')
      WHERE year={int(args.year)} AND NFL_player_id IS NOT NULL
    """)
    # A signal is present only when the source field is non-null.  A positive
    # signal identifies the player as having reached the playoffs; a champion
    # is also necessarily a playoff player.
    con.execute(f"""
      CREATE OR REPLACE TEMP VIEW raw_player AS
      SELECT p.db_name, p.year, p.NFL_player_id,
             SUM(CASE WHEN COALESCE(p.is_started,0)=1 THEN 1 ELSE 0 END)::INTEGER AS starts,
             MAX(CASE WHEN COALESCE(p.is_started,0)=1 AND {championship_signal}=1 THEN 1 ELSE 0 END)::INTEGER AS champ,
             MAX(CASE WHEN COALESCE(p.is_started,0)=1 AND (p.is_playoffs=1
                           OR p.made_playoffs=1
                           OR (p.final_playoff_seed IS NOT NULL AND p.final_playoff_seed<=m.po_slots)
                           OR {championship_signal}=1) THEN 1 ELSE 0 END)::INTEGER AS playoffs,
             MAX(CASE WHEN COALESCE(p.is_started,0)=1 AND (p.is_playoffs IS NOT NULL
                           OR p.made_playoffs IS NOT NULL OR p.final_playoff_seed IS NOT NULL
                           OR p.champion=1) THEN 1 ELSE 0 END)::INTEGER AS resolved
      FROM corp.public.player_fantasy p
      JOIN (SELECT DISTINCT db_name, year, NFL_player_id, po_slots FROM panel_map) m
        ON m.db_name=p.db_name AND m.year=p.year AND m.NFL_player_id=p.NFL_player_id
      WHERE p.year={int(args.year)}
      GROUP BY p.db_name, p.year, p.NFL_player_id
    """)
    con.execute(f"""
      COPY (
        SELECT m.db_name, m.year, m.NFL_player_id, m.player,
               COALESCE(b.nfl_position, 'UNK') AS position, m.teams, m.roster,
               m.scoring, m.pass_td, m.po_slots, m.lineup,
               r.starts, r.champ, r.playoffs, r.resolved
        FROM panel_map m JOIN raw_player r USING (db_name,year,NFL_player_id)
        LEFT JOIN (SELECT DISTINCT NFL_player_id, nfl_position
                   FROM ops.nfl_historical.player_bio) b USING (NFL_player_id)
        WHERE r.starts>0
      ) TO '{str(args.out).replace("'", "''")}' (FORMAT PARQUET)
    """)
    print(con.execute("SELECT COUNT(*), COUNT(DISTINCT db_name), COUNT(*) FILTER (WHERE resolved=1), COUNT(*) FILTER (WHERE playoffs=1) FROM read_parquet(?)", [str(args.out)]).fetchone())
    con.close()


if __name__ == "__main__":
    main()
