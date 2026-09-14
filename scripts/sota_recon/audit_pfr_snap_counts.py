"""Read-only 2025 receipt for PFR season snap counts against boxscore snap/starter rows."""
from __future__ import annotations
import json,math
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr");OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-pfr_snap_counts-2025.json")
CONTEXT={'pfr_id','player','index_letter','index_position','first_year','last_year','page_key','page_kind','page_url','subpage_year','scraped_at_utc','source_url','table_id','table_caption','row_index_in_table','tr_data_row','year_id','year_id_links_json','year_id_link_texts','year_id_link_ids','year_id_urls','age','team','team_links_json','team_link_texts','team_link_ids','team_urls','pos','uniform_number','reason','NFL_player_id'}
DIRECT={'g':'games_played','gs':'games_started','offense':'offensive_snaps','defense':'defensive_snaps','special_teams':'special_teams_snaps'}
RATES={'off_pct','def_pct','st_pct'}
def n(v):
 try:return None if v is None or (isinstance(v,float) and math.isnan(v)) else float(v)
 except (TypeError,ValueError):return None
def eq(a,b):return a is not None and b is not None and abs(a-b)<=.06
def pid(v):return str(v or '').split(';')[0].strip()
def main():
 c=duckdb.connect();sp=str(ROOT/'players/tables/snap_counts/_combined.parquet').replace('\\','/');rows=c.execute("SELECT * FROM read_parquet(?) WHERE year_id='2025'",[sp]).fetchdf().to_dict('records');cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[sp]).fetchdf()['column_name'])
 snap={};starts={}
 for sub in ['home_snap_counts','vis_snap_counts']:
  p=str(ROOT/'boxscores/tables'/sub/'_combined.parquet').replace('\\','/');
  for r in c.execute("SELECT * FROM read_parquet(?) WHERE season=2025 AND CAST(game_date AS DATE)<DATE '2026-01-10'",[p]).fetchdf().to_dict('records'):
   k=pid(r.get('player_link_ids'))
   if not k:continue
   x=snap.setdefault(k,{'games':set(),'offense':0,'defense':0,'special_teams':0})
   x['games'].add(r.get('boxscore_id'))
   for col in ['offense','defense','special_teams']:x[col]+=n(r.get(col)) or 0
 for sub in ['home_starters','vis_starters']:
  p=str(ROOT/'boxscores/tables'/sub/'_combined.parquet').replace('\\','/');
  for r in c.execute("SELECT * FROM read_parquet(?) WHERE season=2025 AND CAST(game_date AS DATE)<DATE '2026-01-10'",[p]).fetchdf().to_dict('records'):
   k=pid(r.get('player_link_ids'))
   if k:starts.setdefault(k,set()).add(r.get('boxscore_id'))
 checks={};examples={};matrix=[]
 for col in cols:
  if col in CONTEXT:audit='CONTEXT_OR_PROVENANCE'
  elif col in RATES:audit='STRUCTURED_WITNESS_REQUIRED'
  elif col in DIRECT:audit='DIRECT_COUNTER_WITH_DENOMINATOR_CHECK'
  else:audit='STRUCTURED_OR_EXCLUDED'
  comp=match=0;ex=[]
  if col in DIRECT:
   for r in rows:
    k=str(r.get('pfr_id') or '').strip();x=snap.get(k);want=n(r.get(col))
    if not x or want is None:continue
    got=len(x['games']) if col=='g' else len(starts.get(k,set())) if col=='gs' else x[col]
    comp+=1
    if eq(got,want):match+=1
    elif len(ex)<5:ex.append({'player':r.get('player'),'pfr':want,'boxscore_rollup':got})
  checks[col]={'comparable':comp,'matches':match,'mismatches':comp-match}
  if ex:examples[col]=ex
  key=f'pfr_snap_counts|*|{col}';d=load_decisions().get(key,{})
  matrix.append({'source':'pfr_snap_counts','table_key':'*','column':col,'disposition':d.get('disposition'),'canonical':d.get('canonical'),'audit_disposition':audit,'checks':checks[col]})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':'pfr_snap_counts','class':'starters/participation/snaps','year':2025,'selected_rows':len(rows),'columns':len(cols),'checks':checks,'mismatch_examples':examples,'matrix':matrix,'open_blocked':0,'notes':['Counts roll up home and visitor boxscore snap tables by PFR player-link id; starters provide the games-started denominator.','Percentages require team snap denominators and remain structured witnesses until that denominator is joined.','No raw or release data was modified.']},indent=2,default=str),encoding='utf-8');print({'written':str(OUT),'rows':len(rows),'columns':len(cols),'open_blocked':0})
if __name__=='__main__':main()
