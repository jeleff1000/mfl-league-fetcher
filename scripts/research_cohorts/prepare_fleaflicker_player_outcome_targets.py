"""Materialize the exact Fleaflicker started-player rows with win IS NULL."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


EXPECTED = 392_546


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', type=Path, required=True)
    ap.add_argument('--targets', type=Path, required=True)
    ap.add_argument('--manifest', type=Path, required=True)
    ap.add_argument('--expected', type=int, default=EXPECTED)
    args = ap.parse_args()
    con = duckdb.connect(str(args.snapshot), read_only=True)
    pc = {r[0] for r in con.execute('DESCRIBE public.player_fantasy').fetchall()}
    sc = {r[0] for r in con.execute('DESCRIBE public.league_settings').fetchall()}
    reqp = {'db_name','year','week','NFL_player_id','manager','is_started','win'}
    reqs = {'db_name','year','platform','league_key','playoff_start_week'}
    missing = {'player_fantasy': sorted(reqp-pc), 'league_settings': sorted(reqs-sc)}
    if any(missing.values()):
        raise SystemExit(f'canonical schema missing required columns: {missing}')
    sql = '''
      SELECT ROW_NUMBER() OVER (ORDER BY p.db_name,p.year,p.week,p.NFL_player_id,p.manager)-1 AS target_ordinal,
             CAST(p.db_name AS VARCHAR) AS db_name, CAST(p.year AS INTEGER) AS year,
             CAST(p.week AS INTEGER) AS week, CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
             CAST(p.manager AS VARCHAR) AS manager, CAST(s.league_key AS VARCHAR) AS source_id,
             TRY_CAST(s.playoff_start_week AS INTEGER) AS playoff_start_week
      FROM public.player_fantasy p
      JOIN public.league_settings s ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
      WHERE LOWER(CAST(s.platform AS VARCHAR))='fleaflicker'
        AND CAST(p.is_started AS INTEGER)=1 AND p.win IS NULL
      ORDER BY target_ordinal
    '''
    args.targets.parent.mkdir(parents=True, exist_ok=True)
    con.execute('COPY ('+sql+') TO ? (FORMAT PARQUET, COMPRESSION ZSTD)', [str(args.targets)])
    count = int(con.execute('SELECT COUNT(*) FROM read_parquet(?)',[str(args.targets)]).fetchone()[0])
    null_source = int(con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE source_id IS NULL OR TRIM(source_id)=''",[str(args.targets)]).fetchone()[0])
    dup = int(con.execute('SELECT COUNT(*)-COUNT(DISTINCT target_ordinal) FROM read_parquet(?)',[str(args.targets)]).fetchone()[0])
    groups = con.execute('''
      SELECT db_name,year,week,MAX(source_id),MAX(playoff_start_week),COUNT(*),MIN(target_ordinal),MAX(target_ordinal)
      FROM read_parquet(?) GROUP BY 1,2,3 ORDER BY 1,2,3
    ''',[str(args.targets)]).fetchall()
    con.close()
    if count != args.expected:
        raise SystemExit(f'fail-closed target count: expected {args.expected}, got {count}')
    if null_source or dup:
        raise SystemExit(f'invalid inventory: null_source={null_source} duplicate_ordinal_excess={dup}')
    manifest = [{
        'db_name':r[0], 'year':int(r[1]), 'week':int(r[2]), 'source_id':r[3],
        'playoff_start_week':int(r[4]) if r[4] is not None else None,
        'target_rows':int(r[5]), 'first_target_ordinal':int(r[6]), 'last_target_ordinal':int(r[7])
    } for r in groups]
    args.manifest.write_text(json.dumps({
        'schema_version':1,
        'population':'canonical public.player_fantasy Fleaflicker started rows where win IS NULL',
        'lineage_id':'research-matchup-v2-final-playoff-anchor-outcomes',
        'target_rows':count, 'group_count':len(manifest), 'groups':manifest
    },indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'target_rows':count,'groups':len(manifest),'null_source':null_source,'duplicate_ordinal_excess':dup},sort_keys=True))


if __name__ == '__main__':
    main()
