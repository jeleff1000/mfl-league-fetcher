from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    args = ap.parse_args()
    c = duckdb.connect(str(args.snapshot), read_only=True)
    c.execute("SET threads=1")
    c.execute("SET preserve_insertion_order=false")
    c.execute("SET temp_directory='/tmp/duckdb_player_coverage'")
    years = [r[0] for r in c.execute("SELECT DISTINCT CAST(year AS INTEGER) FROM public.player_fantasy WHERE db_name IS NOT NULL ORDER BY 1").fetchall()]
    rows = []
    sql = """
      WITH grouped AS (
        SELECT
          p.platform,
          p.db_name,
          CAST(p.year AS INTEGER) AS year,
          CAST(p.week AS INTEGER) AS week,
          CAST(MAX(ls.num_teams) AS INTEGER) AS teams,
          COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1) AS starter_rows,
          COUNT(*) FILTER (WHERE CAST(p.win AS INTEGER)=1) AS winner_rows
        FROM public.player_fantasy p
        LEFT JOIN public.league_settings ls
          ON ls.db_name=p.db_name AND CAST(ls.year AS INTEGER)=CAST(p.year AS INTEGER)
        WHERE p.db_name IS NOT NULL AND CAST(p.year AS INTEGER)=?
        GROUP BY 1,2,3,4
      )
      SELECT *,
        (starter_rows < 3 * teams) AS low_starters,
        (winner_rows < 3 * teams) AS low_winners
      FROM grouped
      WHERE teams IS NOT NULL
      ORDER BY platform, year, week, db_name
    """
    for year in years:
        result_rows = c.execute(sql, [year]).fetchall()
        names = [d[0] for d in c.description]
        rows.extend(dict(zip(names, row)) for row in result_rows)
    data = rows
    def summary(group):
        return {
            'week_year_league_groups': len(group),
            'low_starters_rows': sum(bool(r['low_starters']) for r in group),
            'low_winners_rows': sum(bool(r['low_winners']) for r in group),
        }
    by_platform = {}
    for platform in sorted({r['platform'] for r in data}):
        group = [r for r in data if r['platform'] == platform]
        by_platform[platform] = summary(group)
    by_year = {}
    for year in sorted({r['year'] for r in data}):
        group = [r for r in data if r['year'] == year]
        by_year[str(year)] = summary(group)
    by_platform_year = {}
    for platform in sorted({r['platform'] for r in data}):
        by_platform_year[platform] = {}
        for year in sorted({r['year'] for r in data if r['platform'] == platform}):
            by_platform_year[platform][str(year)] = summary([r for r in data if r['platform'] == platform and r['year'] == year])
    result = {
        'definitions': {
            'unit': 'db_name/year/week',
            'threshold': '3 times league_settings.num_teams',
            'starter_rows': 'player rows where is_started=1',
            'winner_rows': 'player rows where win=1',
            'player_variants': 'distinct NFL_player_id counts, reported separately',
        },
        'groups': data,
        'by_platform': by_platform,
        'by_year': by_year,
        'by_platform_year': by_platform_year,
    }
    args.out.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps({'by_platform': by_platform, 'by_year': by_year}, indent=2))
    c.close()


if __name__ == '__main__':
    main()
