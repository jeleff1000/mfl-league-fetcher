"""Roll up PFR game-level player offense/defense tables into 2025 season receipts."""
from __future__ import annotations
import argparse,json,math
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr"); OUT=Path(r"D:\yahoo_oauth\docs\audits")
SURFACES={'regular':('player_offense','pfr_player_offense_box','offense',['passing','receiving_and_rushing']), 'post':('player_offense','pfr_player_offense_box','offense',['passing_post','receiving_and_rushing_post'])}
DIRECT_OFF={'pass_cmp':'pass_cmp','pass_att':'pass_att','pass_yds':'pass_yds','pass_td':'pass_td','pass_int':'pass_int','pass_sacked':'pass_sacked','pass_sacked_yds':'pass_sacked_yds','pass_long':'pass_long','rush_att':'rush_att','rush_yds':'rush_yds','rush_td':'rush_td','rush_long':'rush_long','rec':'rec','rec_yds':'rec_yds','rec_td':'rec_td','rec_long':'rec_long','targets':'targets','fumbles':'fumbles','fumbles_lost':'fumbles_lost'}
DIRECT_DEF={'def_int':'def_int','def_int_yds':'def_int_yds','def_int_td':'def_int_td','def_int_long':'def_int_long','sacks':'sacks','tackles_combined':'tackles_combined','tackles_solo':'tackles_solo','tackles_assists':'tackles_assists','fumbles_rec':'fumbles_rec','fumbles_rec_yds':'fumbles_rec_yds','fumbles_rec_td':'fumbles_rec_td','fumbles_forced':'fumbles_forced','pass_defended':'pass_defended','tackles_loss':'tackles_loss','qb_hits':'qb_hits'}
CONTEXT={'boxscore_id','boxscore_url','game_date','season','home_stathead_id','source_url','table_id','table_caption','row_index_in_table','tr_data_row','player','player_links_json','player_link_texts','player_link_ids','player_urls','team'}
def num(v):
 try:return None if v is None or (isinstance(v,float) and math.isnan(v)) else float(v)
 except (TypeError,ValueError):return None
def eq(a,b):return a is not None and b is not None and abs(a-b)<=.06
def pid(row):
 v=row.get('player_link_ids')
 if v is None:return None
 s=str(v).strip(); return s.split(';')[0].strip() or None
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--surface',choices=['regular','post'],default='regular');ap.add_argument('--kind',choices=['offense','defense'],default='offense');a=ap.parse_args()
 game_dir,_,_,season_dirs=SURFACES[a.surface]
 if a.kind=='defense': game_dir='player_defense'; source='pfr_player_defense_box'; season_dirs=['defense_post' if a.surface=='post' else 'defense']
 else: source='pfr_player_offense_box'
 c=duckdb.connect(); game_path=str(ROOT/'boxscores'/'tables'/game_dir/'_combined.parquet').replace('\\','/'); cutoff="CAST(game_date AS DATE) >= DATE '2026-01-10'" if a.surface=='post' else "CAST(game_date AS DATE) < DATE '2026-01-10'"
 games=c.execute(f"SELECT * FROM read_parquet(?) WHERE season=2025 AND {cutoff}",[game_path]).fetchdf().to_dict('records')
 cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[game_path]).fetchdf()['column_name']); direct=DIRECT_OFF if a.kind=='offense' else DIRECT_DEF
 # Aggregate game rows by stable PFR link id and team. Blank/statless rows remain part of the
 # grain but contribute no counter; this preserves the source's NULL-vs-zero semantics in checks.
 ag={}
 for r in games:
  k=(pid(r),str(r.get('team') or '').strip().lower())
  if not k[0]: continue
  x=ag.setdefault(k,{col:0.0 for col in direct}); x['_rows']=x.get('_rows',0)+1
  for col in direct:
   n=num(r.get(col));
   if n is not None:
    if col.endswith('_long'): x[col]=max(x[col],n)
    else: x[col]+=n
 # Season surfaces: use 2TM rows as an all-team aggregate, otherwise the matching team row.
 season={};
 for sd in season_dirs:
  sp=str(ROOT/'players'/'tables'/sd/'_combined.parquet').replace('\\','/')
  srows=c.execute("SELECT * FROM read_parquet(?) WHERE year_id='2025'",[sp]).fetchdf().to_dict('records')
  for r in srows:
   k=str(r.get('pfr_id') or '').strip()
   if not k:continue
   team=str(r.get('team_name_abbr') or '').strip().lower(); season[(k,team)]=r
 decisions=load_decisions();checks={};examples={};matrix=[]
 for col in cols:
  if col in CONTEXT:audit='CONTEXT_OR_PROVENANCE'
  elif col in {'pass_rating'}:audit='STRUCTURED_WITNESS_REQUIRED'
  elif col in direct:audit='GAME_TO_SEASON_DIRECT_ROLLUP'
  else:audit='STRUCTURED_OR_EXCLUDED'
  comp=match=0;ex=[]
  if col in direct:
   for (pfr_id,team),sr in season.items():
    if team=='2tm':
     candidates=[(k,v) for k,v in ag.items() if k[0]==pfr_id]; got=(max(v.get(col,0.0) for _,v in candidates) if col.endswith('_long') else sum(v.get(col,0.0) for _,v in candidates)) if candidates else None
    else: got=ag.get((pfr_id,team),{}).get(col) if (pfr_id,team) in ag else None
    want=num(sr.get(col))
    # PFR season long fields use negative sentinels for an unpublished/no-long value;
    # they are not numeric lengths and must not be compared with a game rollup of zero.
    if col.endswith('_long') and want is not None and want < 0: continue
    if got is None or want is None:continue
    comp+=1
    if eq(got,want):match+=1
    elif len(ex)<5:ex.append({'pfr_id':pfr_id,'team':team,'season':want,'game_rollup':got})
  checks[col]={'comparable':comp,'matches':match,'mismatches':comp-match}
  if ex:examples[col]=ex
  key=f'{source}|*|{col}';d=decisions.get(key,{})
  matrix.append({'source':source,'table_key':'*','column':col,'disposition':d.get('disposition'),'canonical':d.get('canonical'),'audit_disposition':audit,'checks':checks[col]})
 out=OUT/f'pfr-{source}-{a.kind}-{a.surface}-2025.json';out.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':source,'kind':a.kind,'surface':a.surface,'year':2025,'selected_game_rows':len(games),'season_rows':len(season),'columns':len(cols),'checks':checks,'mismatch_examples':examples,'matrix':matrix,'open_blocked':0,'notes':['Game rows are rolled up by PFR player-link id and team; PFR 2TM season rows aggregate all matching teams.','No raw or release data was modified.','Rates and nonlinear game metrics are structured witnesses rather than summed.']},indent=2,default=str),encoding='utf-8');print({'written':str(out),'game_rows':len(games),'season_rows':len(season),'columns':len(cols),'open_blocked':0})
if __name__=='__main__':main()
