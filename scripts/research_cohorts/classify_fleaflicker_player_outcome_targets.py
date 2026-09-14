"""Classify exact Fleaflicker player-week outcome gaps from source scoreboards."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd

from multi_league.data_fetchers.fleaflicker.fleaflicker_api_client import FleaflickerAPIClient
from multi_league.data_fetchers.fleaflicker.fleaflicker_matchups import rows_for_game
from multi_league.data_fetchers.fleaflicker.fleaflicker_utils import clean_id, team_lookup_from_standings


def selected(db: str, year: int, week: int, shard: int, shards: int) -> bool:
    return int(hashlib.sha256(f'{db}|{year}|{week}'.encode()).hexdigest()[:12],16)%shards==shard


def norm(value: object) -> str:
    return ' '.join(str(value or '').strip().lower().split())


def main() -> None:
    ap=argparse.ArgumentParser()
    ap.add_argument('--targets',type=Path,required=True); ap.add_argument('--manifest',type=Path,required=True)
    ap.add_argument('--shard',type=int,required=True); ap.add_argument('--shards',type=int,required=True); ap.add_argument('--out',type=Path,required=True)
    args=ap.parse_args()
    manifest=json.loads(args.manifest.read_text(encoding='utf-8'))
    groups=[g for g in manifest['groups'] if selected(str(g['db_name']),int(g['year']),int(g['week']),args.shard,args.shards)]
    con=duckdb.connect(); df=con.execute('SELECT * FROM read_parquet(?)',[str(args.targets)]).fetchdf(); con.close()
    wanted={(str(g['db_name']),int(g['year']),int(g['week'])) for g in groups}
    df=df[df.apply(lambda r:(str(r.db_name),int(r.year),int(r.week)) in wanted,axis=1)]
    client=FleaflickerAPIClient()
    season_cache={}
    results=[]
    for g in groups:
        db,year,week=str(g['db_name']),int(g['year']),int(g['week']); source_id=str(g.get('source_id') or '')
        subset=df[(df.db_name.astype(str)==db)&(df.year.astype(int)==year)&(df.week.astype(int)==week)]
        try:
            if not source_id: raise RuntimeError('missing_source_id')
            key=(source_id,year)
            if key not in season_cache:
                standings=client.fetch_standings(source_id,season=year) or {}
                lookup=team_lookup_from_standings(standings)
                manager_to_team={}
                for tid,team in lookup.items():
                    owners=team.get('owners') or []
                    if isinstance(owners,dict): owners=[owners]
                    names=[team.get('name')]
                    names += [o.get('displayName') or o.get('display_name') or o.get('name') for o in owners if isinstance(o,dict)]
                    for name in names:
                        if norm(name): manager_to_team[norm(name)]=str(tid)
                season_cache[key]=(lookup,manager_to_team)
            lookup,manager_to_team=season_cache[key]
            scoreboard=client.fetch_scoreboard(source_id,season=year,scoring_period=week) or {}
            games=scoreboard.get('games') or []
            source_rows=[]
            for game in games:
                if isinstance(game,dict): source_rows.extend(rows_for_game(game,year,week,source_id,lookup))
            by_manager={norm(r.get('manager')):r for r in source_rows if norm(r.get('manager'))}
            game_team_ids={clean_id(r.get('team_key')) for r in source_rows if clean_id(r.get('team_key'))}
            is_playoff=any(bool(r.get('is_playoffs')) for r in source_rows) or bool(g.get('playoff_start_week') and week>=int(g['playoff_start_week']))
            for _,t in subset.iterrows():
                manager=norm(t.get('manager')); source=by_manager.get(manager)
                if source is not None and source.get('opponent_franchise_id') and source.get('opponent'):
                    row={**t.to_dict(),'status':'confirmed_outcome','source_id':source_id,'source_win':source.get('win'),'source_loss':source.get('loss'),'source_tie':source.get('tie'),'source_team_points':source.get('team_points'),'source_opponent_points':source.get('opponent_points'),'source_is_playoffs':source.get('is_playoffs')}
                elif not manager:
                    row={**t.to_dict(),'status':'identity_missing','source_id':source_id,'source_games':len(games)}
                elif is_playoff and norm(manager) in manager_to_team:
                    row={**t.to_dict(),'status':'eliminated_or_no_opponent','source_id':source_id,'source_games':len(games)}
                elif not is_playoff and norm(manager) in manager_to_team:
                    row={**t.to_dict(),'status':'regular_week_no_source_game','source_id':source_id,'source_games':len(games)}
                elif manager not in manager_to_team:
                    row={**t.to_dict(),'status':'source_identity_unmatched','source_id':source_id,'source_games':len(games)}
                else:
                    row={**t.to_dict(),'status':'source_week_no_game','source_id':source_id,'source_games':len(games)}
                results.append(row)
        except Exception as exc:
            results.extend({**t.to_dict(),'status':'source_error','source_id':source_id,'error':repr(exc)} for _,t in subset.iterrows())
    out=pd.DataFrame(results); args.out.parent.mkdir(parents=True,exist_ok=True); out.to_parquet(args.out,index=False)
    print(json.dumps({'shard':args.shard,'groups':len(groups),'rows':len(out),'statuses':out.status.value_counts().to_dict() if len(out) else {}},sort_keys=True))


if __name__=='__main__': main()
