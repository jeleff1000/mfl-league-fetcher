"""Derive missing row-level championship signals from retained matchup evidence.

The folded lake can retain an eventual champion marker and the matchup graph while
losing the explicit title-game flag.  The season winner marker alone is not title
evidence, so this repair uses it only to locate that team's final playoff matchup,
then marks both sides of that matchup.  A verified multi-week final also marks its
preceding leg when the same pair appears there.

This is an immutable sidecar operation: it does not mutate the source lake.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def cols(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()}


def build_sidecar(con: duckdb.DuckDBPyConnection, base: str = "base") -> int:
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    matchup = f"{base}.public.matchup"
    player = f"{base}.public.player_fantasy"
    settings = f"{base}.public.league_settings"
    mcols = cols(con, matchup)
    pcols = cols(con, player)
    lscols = cols(con, settings)
    required = {"db_name", "year", "week", "manager"}
    if not required <= mcols:
        raise ValueError(f"matchup missing required columns: {sorted(required - mcols)}")

    team_key = "m.team_key" if "team_key" in mcols else "NULL::VARCHAR"
    opponent = "m.opponent" if "opponent" in mcols else "NULL::VARCHAR"
    opponent_team = "m.opponent_team_key" if "opponent_team_key" in mcols else "NULL::VARCHAR"
    matchup_key = "m.matchup_key" if "matchup_key" in mcols else "NULL::VARCHAR"
    m_champion = "CAST(m.champion AS INTEGER)=1" if "champion" in mcols else "FALSE"
    m_playoff = "CAST(m.is_playoffs AS INTEGER)=1" if "is_playoffs" in mcols else "TRUE"

    franchise_key = "m.franchise_id" if "franchise_id" in mcols else "NULL::VARCHAR"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE _champion_sources AS
      SELECT DISTINCT
             CAST(m.db_name AS VARCHAR) AS db_name,
             CAST(m.year AS INTEGER) AS year,
             CAST(m.manager AS VARCHAR) AS manager,
             CAST({team_key} AS VARCHAR) AS team_key,
             CAST({franchise_key} AS VARCHAR) AS franchise_id
      FROM {matchup} m
      WHERE {m_champion} AND {m_playoff}
    """)
    matchup_source_count = int(con.execute("SELECT COUNT(*) FROM _champion_sources").fetchone()[0])
    player_source_count = 0

    # Some older folds retain the champion flag only on player rows.  Join those
    # rows back to the matchup graph so the same final-game rule still applies.
    if "champion" in pcols:
        # Use the matchup-side identity for the title-game candidate.  The
        # player fold's team_key may be a platform-native identifier from a
        # different normalization pass; the join has already established the
        # correct matchup team.
        p_team = "m.team_key" if "team_key" in mcols else "NULL::VARCHAR"
        p_started = "CAST(p.is_rostered AS INTEGER)=1" if "is_rostered" in pcols else "TRUE"
        join_team = (
            "((p.team_key IS NOT NULL AND m.team_key IS NOT DISTINCT FROM p.team_key) "
            "OR LOWER(TRIM(CAST(m.manager AS VARCHAR))) = LOWER(TRIM(CAST(p.manager AS VARCHAR))))"
            if "team_key" in pcols and "team_key" in mcols
            else "LOWER(TRIM(CAST(m.manager AS VARCHAR))) = LOWER(TRIM(CAST(p.manager AS VARCHAR)))"
        )
        con.execute(f"""
          INSERT INTO _champion_sources
          SELECT DISTINCT
                 CAST(p.db_name AS VARCHAR), CAST(p.year AS INTEGER),
                 CAST(m.manager AS VARCHAR), CAST({p_team} AS VARCHAR),
                 CAST({franchise_key} AS VARCHAR)
          FROM {player} p
          JOIN {matchup} m
            ON m.db_name=p.db_name AND CAST(m.year AS INTEGER)=CAST(p.year AS INTEGER)
           AND CAST(m.week AS INTEGER)=CAST(p.week AS INTEGER) AND {join_team}
          WHERE CAST(p.champion AS INTEGER)=1 AND {p_started} AND {m_playoff}
        """)
        player_source_count = int(con.execute("SELECT COUNT(*) FROM _champion_sources").fetchone()[0]) - matchup_source_count
        total_player_champions = int(con.execute(
            f"SELECT COUNT(*) FROM {player} p WHERE CAST(p.champion AS INTEGER)=1 AND {p_started}"
        ).fetchone()[0])
        manager_joined_player_champions = int(con.execute(f"""
            SELECT COUNT(*) FROM {player} p
            WHERE CAST(p.champion AS INTEGER)=1 AND {p_started}
              AND EXISTS (
                SELECT 1 FROM {matchup} m
                WHERE m.db_name=p.db_name AND CAST(m.year AS INTEGER)=CAST(p.year AS INTEGER)
                  AND CAST(m.week AS INTEGER)=CAST(p.week AS INTEGER)
                  AND LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(CAST(p.manager AS VARCHAR)))
              )
        """).fetchone()[0])
    else:
        total_player_champions = 0
        manager_joined_player_champions = 0

    multiweek = (
        "COALESCE(CAST(ls.has_multiweek_championship AS INTEGER),0)"
        if "has_multiweek_championship" in lscols
        else "0"
    )
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE _final_champions AS
      SELECT db_name, year, week, manager, team_key, opponent, opponent_team_key,
             matchup_key, multiweek
      FROM (
        SELECT c.db_name, c.year,
               CAST(m.week AS INTEGER) AS week,
               CAST(m.manager AS VARCHAR) AS manager,
               CAST({team_key.replace('m.', 'm.')} AS VARCHAR) AS team_key,
               CAST({opponent} AS VARCHAR) AS opponent,
               CAST({opponent_team} AS VARCHAR) AS opponent_team_key,
               CAST({matchup_key} AS VARCHAR) AS matchup_key,
               {multiweek} AS multiweek,
               ROW_NUMBER() OVER (PARTITION BY c.db_name,c.year ORDER BY CAST(m.week AS INTEGER) DESC) AS rn
        FROM _champion_sources c
        JOIN {matchup} m
          ON m.db_name=c.db_name AND CAST(m.year AS INTEGER)=c.year
         AND {m_playoff.replace('m.', 'm.')}
         AND (
              (c.franchise_id IS NOT NULL AND CAST(m.franchise_id AS VARCHAR)=c.franchise_id)
              OR (c.franchise_id IS NULL AND c.team_key IS NOT NULL
                  AND CAST(m.team_key AS VARCHAR)=c.team_key)
              OR (c.franchise_id IS NULL AND c.team_key IS NULL
                  AND LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(c.manager)))
         )
        LEFT JOIN {settings} ls ON ls.db_name=c.db_name AND CAST(ls.year AS INTEGER)=c.year
      )
      WHERE rn=1
    """)

    # Add the preceding leg only when it is the same pair. This captures genuine
    # two-week finals without mistaking a semifinal for a title game.
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE _title_games AS
      SELECT db_name, year, week, manager, team_key, opponent, opponent_team_key, matchup_key
      FROM _final_champions
      UNION
      SELECT f.db_name, f.year, m.week, f.manager, f.team_key, f.opponent, f.opponent_team_key, m.matchup_key
      FROM _final_champions f
      JOIN {matchup} m
       ON m.db_name=f.db_name AND CAST(m.year AS INTEGER)=f.year AND CAST(m.week AS INTEGER)=f.week-1
       AND (
          (CAST(m.team_key AS VARCHAR) IS NOT NULL AND f.team_key IS NOT NULL
           AND CAST(m.opponent_team_key AS VARCHAR) IS NOT NULL AND f.opponent_team_key IS NOT NULL
           AND CAST(m.team_key AS VARCHAR)=f.team_key
           AND CAST(m.opponent_team_key AS VARCHAR)=f.opponent_team_key)
          OR (CAST(m.team_key AS VARCHAR) IS NOT NULL AND f.opponent_team_key IS NOT NULL
           AND CAST(m.opponent_team_key AS VARCHAR) IS NOT NULL AND f.team_key IS NOT NULL
           AND CAST(m.team_key AS VARCHAR)=f.opponent_team_key
           AND CAST(m.opponent_team_key AS VARCHAR)=f.team_key)
          OR (f.manager IS NOT NULL AND f.opponent IS NOT NULL
           AND LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(f.manager))
           AND LOWER(TRIM(CAST(m.opponent AS VARCHAR)))=LOWER(TRIM(f.opponent)))
       )
      WHERE f.multiweek=1
    """)

    m_franchise = "m.franchise_id" if "franchise_id" in mcols else "NULL::VARCHAR"
    con.execute(f"""
      CREATE OR REPLACE TABLE public.championship_signal_sidecar AS
      SELECT DISTINCT CAST(m.db_name AS VARCHAR) AS db_name,
             CAST(m.year AS INTEGER) AS year, CAST(m.week AS INTEGER) AS week,
             CAST(m.manager AS VARCHAR) AS manager,
             CAST({m_franchise} AS VARCHAR) AS franchise_id,
             1::INTEGER AS is_championship
      FROM {matchup} m
      JOIN _title_games t
       ON m.db_name=t.db_name AND CAST(m.year AS INTEGER)=t.year AND CAST(m.week AS INTEGER)=t.week
       AND (
          (CAST(m.matchup_key AS VARCHAR) IS NOT NULL AND t.matchup_key IS NOT NULL
           AND CAST(m.matchup_key AS VARCHAR)=t.matchup_key)
          OR (CAST(m.team_key AS VARCHAR) IS NOT NULL AND t.team_key IS NOT NULL
              AND CAST(m.opponent_team_key AS VARCHAR) IS NOT NULL AND t.opponent_team_key IS NOT NULL
              AND CAST(m.team_key AS VARCHAR)=t.team_key
              AND CAST(m.opponent_team_key AS VARCHAR)=t.opponent_team_key)
          OR (CAST(m.team_key AS VARCHAR) IS NOT NULL AND t.opponent_team_key IS NOT NULL
              AND CAST(m.opponent_team_key AS VARCHAR) IS NOT NULL AND t.team_key IS NOT NULL
              AND CAST(m.team_key AS VARCHAR)=t.opponent_team_key
              AND CAST(m.opponent_team_key AS VARCHAR)=t.team_key)
          OR (t.manager IS NOT NULL AND LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(t.manager))
              AND (t.opponent IS NULL OR LOWER(TRIM(CAST(m.opponent AS VARCHAR)))=LOWER(TRIM(t.opponent))))
          OR (t.manager IS NOT NULL AND t.opponent IS NOT NULL
              AND LOWER(TRIM(CAST(m.manager AS VARCHAR)))=LOWER(TRIM(t.opponent))
              AND LOWER(TRIM(CAST(m.opponent AS VARCHAR)))=LOWER(TRIM(t.manager)))
       )
    """)
    sidecar_count = int(con.execute("SELECT COUNT(*) FROM public.championship_signal_sidecar").fetchone()[0])
    final_count = int(con.execute("SELECT COUNT(*) FROM _final_champions").fetchone()[0])
    title_game_count = int(con.execute("SELECT COUNT(*) FROM _title_games").fetchone()[0])
    print({
        "matchup_champion_sources": matchup_source_count,
        "player_champion_rows_total": total_player_champions,
        "player_champion_rows_manager_joined": manager_joined_player_champions,
        "player_champion_sources_added": player_source_count,
        "final_champion_teams": final_count,
        "title_game_candidates": title_game_count,
        "sidecar_rows": sidecar_count,
    })
    return sidecar_count


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.out.exists():
        args.out.unlink()
    con = duckdb.connect(str(args.out))
    con.execute(f"ATTACH '{args.base.resolve().as_posix().replace(chr(39), chr(39)*2)}' AS base (READ_ONLY)")
    count = build_sidecar(con)
    con.close()
    print({"sidecar_rows": count, "output": str(args.out)})


if __name__ == "__main__":
    main()
