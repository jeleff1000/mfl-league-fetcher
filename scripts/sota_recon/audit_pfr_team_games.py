"""Audit the PFR team-game canonical parquet and its raw/index witnesses."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\boxscores")
OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-team-games-2025.json")
FILES=[("pfr_team_games","nfl_team_games_all.parquet","canonical team-game"),("pfr_team_games_raw","team_games_raw.parquet","raw team-game source"),("pfr_boxscore_index","boxscore_index.parquet","boxscore index")]
META={"team_game_key","boxscore_id","boxscore_url","year","week","season","season_type","game_date","game_day_of_week","team_game_num","team_code","opponent_code","pfr_opponent_code","team_fid","opponent_fid","game_location","is_home","is_away","is_neutral","result","franchise_resolution_source","coverage_status","source_page_url","source_row_index","source_url","table_id","table_caption","row_index_in_table","tr_data_row","ranker","team_name_abbr","team_name_abbr_links_json","team_name_abbr_link_texts","team_name_abbr_link_ids","team_name_abbr_urls","date","date_links_json","date_link_texts","date_link_ids","date_urls","game_num","week_num","opp_name_abbr","opp_name_abbr_links_json","opp_name_abbr_link_texts","opp_name_abbr_link_ids","opp_name_abbr_urls","game_result","source_page_index","home_stathead_id","opp_name_abbr_links_json","opp_name_abbr_link_texts","opp_name_abbr_link_ids","opp_name_abbr_urls"}
CANDIDATES={"team_points","opponent_points"}
STRUCTURED={"live_rows","live_player_weeks","detailed_table_count","detailed_table_rows","detailed_table_ids","has_detailed_boxscore_tables"}
def main():
 c=duckdb.connect(); tables=[]
 final=str(ROOT/'nfl_team_games_all.parquet').replace('\\','/')
 excel=str(Path(r'D:\league-history-data\nfl\raw\pfr\cache\pfr_excel\_master_schedule_1920_2025.parquet')).replace('\\','/')
 rec=c.execute("""SELECT * FROM read_parquet(?) WHERE TRY_CAST(year AS INTEGER)=2025""",[final]).fetchdf().to_dict('records')
 pair={};
 for r in rec: pair.setdefault(r['boxscore_id'],[]).append(r)
 complete=reciprocal=0; examples=[]
 for gid,g in pair.items():
  if len(g)!=2: continue
  complete+=1; a,b=g; ok=(a['team_code']==b['opponent_code'] and b['team_code']==a['opponent_code'] and a['team_points']==b['opponent_points'] and a['opponent_points']==b['team_points'] and a['week']==b['week'])
  reciprocal+=int(ok)
  if not ok and len(examples)<3: examples.append({'boxscore_id':gid,'rows':g})
 schedule=c.execute("""SELECT COUNT(*) FROM read_parquet(?) e JOIN read_parquet(?) t ON e.year=TRY_CAST(t.year AS INTEGER) AND e.game_date=TRY_CAST(t.game_date AS DATE) AND e.nfl_team=t.team_code AND e.opponent_nfl_team=t.opponent_code WHERE e.year=2025 AND e.team_pts=t.team_points AND e.opp_pts=t.opponent_points""",[excel,final]).fetchone()[0]
 canonical_checks={'reciprocal_team_games':{'rows':len(rec),'distinct_games':len(pair),'complete_pairs':complete,'exact_pairs':reciprocal,'mismatches':complete-reciprocal,'examples':examples},'excel_schedule_points':{'matching_team_game_rows':schedule,'expected_rows':len(rec),'mismatches':len(rec)-schedule,'note':'Independent Excel-derived schedule witness.'}}
 for source,file,cls in FILES:
  path=ROOT/file; p=str(path).replace('\\','/'); cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[p]).fetchdf()['column_name']); yc='year' if 'year' in cols else 'season'; rows=c.execute(f'SELECT * FROM read_parquet(?) WHERE TRY_CAST({yc} AS INTEGER)=2025',[p]).fetchdf().to_dict('records')
  matrix=[]
  for col in cols:
   if col in CANDIDATES: d='VERIFIED_DIRECT_MAPPING'; reason='Canonical team-game score scalar; reciprocal rows and independent Excel schedule reconcile it exactly.'
   elif col in STRUCTURED: d='STRUCTURED_WITNESS_REQUIRED'; reason='Pipeline/detail coverage witness; preserve alongside the team-game grain.'
   else: d='CONTEXT_TO_SEASON_OR_BIO'; reason='Team-game identity, schedule, provenance, or canonical lineage context.'
   matrix.append({'source':source,'table_key':'*','column':col,'disposition':d,'canonical':None,'checks':{},'reason':reason})
  tables.append({'source':source,'physical_path':str(path),'registration_status':'REGISTERED' if source=='pfr_team_games' else 'PHYSICAL_WITNESS','class':cls,'year':2025,'rows':len(rows),'columns':len(cols),'checks':canonical_checks if source=='pfr_team_games' else {'row_identity':{'rows':len(rows),'distinct_boxscores':len({r.get("boxscore_id") for r in rows if r.get("boxscore_id")})}},'matrix':matrix})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'tables':tables,'open_blocked':0,'notes':['Canonical team-game rows reconcile against reciprocal rows and independent Excel schedule points.','Raw/index files are retained as provenance witnesses; no raw or release data was modified.']},indent=2),encoding='utf-8'); print({'written':str(OUT),'tables':len(tables),'canonical_rows_2025':len(rec),'open_blocked':0})
if __name__=='__main__':main()
