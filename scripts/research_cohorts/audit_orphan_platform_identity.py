from __future__ import annotations

import argparse
from pathlib import Path
import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', type=Path, required=True)
    args = ap.parse_args()
    c = duckdb.connect(str(args.snapshot), read_only=True)
    tables = c.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' ORDER BY 1").fetchall()
    print('TABLES', [r[0] for r in tables])
    for (t,) in tables:
        cols = [r[0] for r in c.execute(f'DESCRIBE public."{t}"').fetchall()]
        print('COLUMNS', t, cols)

    pf = {r[0] for r in c.execute('DESCRIBE public.player_fantasy').fetchall()}
    print('PLAYER_NULL_IDENTITY', c.execute("""
      SELECT COUNT(*) AS rows,
             COUNT(*) FILTER (WHERE db_name IS NULL) AS null_db,
             COUNT(*) FILTER (WHERE platform IS NULL) AS null_platform,
             COUNT(*) FILTER (WHERE team_key IS NULL) AS null_team_key,
             COUNT(*) FILTER (WHERE NFL_player_id IS NULL) AS null_nfl_player
      FROM public.player_fantasy
    """).fetchone())
    print('ORPHAN_DISTINCT_PLAYERS', c.execute("""
      SELECT COUNT(DISTINCT NFL_player_id)
      FROM public.player_fantasy
      WHERE db_name IS NULL
    """).fetchone()[0])
    print('ORPHAN_DISTINCT_STARTERS', c.execute("""
      SELECT COUNT(DISTINCT NFL_player_id)
      FROM public.player_fantasy
      WHERE db_name IS NULL AND CAST(is_started AS INTEGER)=1
    """).fetchone()[0])
    print('ORPHAN_DISTINCT_EARLY_STARTERS', c.execute("""
      SELECT COUNT(DISTINCT NFL_player_id)
      FROM public.player_fantasy
      WHERE db_name IS NULL AND CAST(is_started AS INTEGER)=1 AND CAST(week AS INTEGER) BETWEEN 1 AND 13
    """).fetchone()[0])
    print('ORPHAN_EARLY_STARTER_PROFILE', c.execute("""
      SELECT
        COUNT(DISTINCT NFL_player_id) AS players,
        COUNT(DISTINCT NFL_player_id) FILTER (WHERE position IS NOT NULL) AS with_position,
        COUNT(DISTINCT NFL_player_id) FILTER (WHERE fantasy_points IS NOT NULL) AS with_points,
        COUNT(DISTINCT NFL_player_id) FILTER (WHERE COALESCE(fantasy_points,0) <> 0) AS nonzero_points
      FROM public.player_fantasy
      WHERE db_name IS NULL AND CAST(is_started AS INTEGER)=1 AND CAST(week AS INTEGER) BETWEEN 1 AND 13
    """).fetchone())
    print('ORPHAN_BY_YEAR', c.execute("""
      SELECT year,
             COUNT(*) AS row_count,
             COUNT(DISTINCT NFL_player_id) players,
             COUNT(*) FILTER (WHERE CAST(is_rostered AS INTEGER)=1) rostered_rows,
             COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER)=1) started_rows
      FROM public.player_fantasy
      WHERE db_name IS NULL
      GROUP BY 1 ORDER BY 1
    """).fetchall())
    print('PLATFORM_NULL_BY_YEAR', c.execute("""
      SELECT year,
             COUNT(*) AS total_rows,
             COUNT(*) FILTER (WHERE db_name IS NULL) AS null_db_rows,
             ROUND(100.0 * COUNT(*) FILTER (WHERE db_name IS NULL) / NULLIF(COUNT(*),0), 4) AS null_db_pct
      FROM public.player_fantasy
      GROUP BY 1 ORDER BY 1
    """).fetchall())
    ls = {r[0] for r in c.execute('DESCRIBE public.league_settings').fetchall()}
    print('SETTINGS_PLATFORM', c.execute("SELECT platform, COUNT(*) FROM public.league_settings GROUP BY 1 ORDER BY 1").fetchall())
    for t, cols in tables:
        if t == 'player_fantasy':
            continue
        ccols = {r[0] for r in c.execute(f'DESCRIBE public."{t}"').fetchall()}
        shared = sorted(pf & ccols)
        print('SHARED_WITH_PLAYER', t, shared)
        for key in ('team_key', 'db_name', 'NFL_player_id'):
            if key in pf and key in ccols:
                q = f'''SELECT COUNT(*) FROM public.player_fantasy p JOIN public."{t}" x USING ("{key}") WHERE p.db_name IS NULL'''
                try:
                    print('ORPHAN_JOIN', t, key, c.execute(q).fetchone()[0])
                except Exception as e:
                    print('ORPHAN_JOIN_ERROR', t, key, str(e))
    c.close()


if __name__ == '__main__':
    main()
