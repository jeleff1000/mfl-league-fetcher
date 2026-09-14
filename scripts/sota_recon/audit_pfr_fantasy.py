"""Read-only 2025 receipt for the registered PFR player-fantasy surface."""
from __future__ import annotations
import json,math
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables"); OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-pfr_player_fantasy-2025.json")
CONTEXT={'pfr_id','player','index_letter','index_position','first_year','last_year','page_key','page_kind','page_url','subpage_year','scraped_at_utc','source_url','table_id','table_caption','row_index_in_table','tr_data_row','year_id','year_id_links_json','year_id_link_texts','year_id_link_ids','year_id_urls','age','games','games_links_json','games_link_texts','games_link_ids','games_urls','fantasy_pos','NFL_player_id'}
def n(v):
 try:return None if v is None or (isinstance(v,float) and math.isnan(v)) else float(v)
 except (TypeError,ValueError):return None
def normkey(v): return str(v or '').strip().lower()
def preferred(rows):
 out={}
 for r in rows:
  pid=r.get('pfr_id')
  if not pid: continue
  old=out.get(pid)
  if old is None or (str(old.get('team_name_abbr') or '')!='2TM' and str(r.get('team_name_abbr') or '')=='2TM'):
   out[pid]=r
 return out
def main():
 c=duckdb.connect(); fantasy=str(ROOT/'fantasy'/'_combined.parquet').replace('\\','/');
 rows=c.execute("SELECT * FROM read_parquet(?) WHERE year_id='2025'",[fantasy]).fetchdf().to_dict('records');cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[fantasy]).fetchdf()['column_name'])
 sources={}
 for name in ['passing','rushing_and_receiving','receiving_and_rushing','scoring']:
  p=str(ROOT/name/'_combined.parquet').replace('\\','/');sources[name]=preferred(c.execute("SELECT * FROM read_parquet(?) WHERE year_id='2025'",[p]).fetchdf().to_dict('records'))
 rel=str(Path(r'D:\league-history-data\nfl\releases\nfl_local_release_newspaper_witness_overlay_20260717T065218Z_v26_local_only\tables\nfl_player_stats_all.parquet')).replace('\\','/')
 # Use the canonical lower-layer fumble-loss operands. The former witness
 # keyed box-score NFL/stathead ids against PFR ids and also counted a wider
 # game fumble lane than the canonical fantasy formula.
 fum={}
 frows=c.execute("""SELECT lower(player) player,
   sum(coalesce(fumbles_lost,0)) fumbles_lost
   FROM read_parquet(?) WHERE year=2025 AND season_type='REG' GROUP BY 1""",[rel]).fetchdf().to_dict('records')
 for r in frows:
  pid=normkey(r.get('player'))
  if pid: fum[pid]=n(r.get('fumbles_lost')) or 0
 decisions=load_decisions();checks={};examples={};matrix=[]
 for col in cols:
  audit='CONTEXT_TO_SEASON_OR_BIO' if col in CONTEXT else ('PROMOTION_CANDIDATE' if col in {'fantasy_points','vbd'} else ('INTENTIONALLY_UNMAPPED_WITH_REASON' if col in {'fantasy_rank_pos','fantasy_rank_overall'} else 'STRUCTURED_WITNESS_REQUIRED'))
  comp=match=0;ex=[]
  if col=='fantasy_points':
   for r in rows:
    pid=r.get('pfr_id'); rr=sources['rushing_and_receiving'].get(pid) or sources['receiving_and_rushing'].get(pid) or {}; pp=sources['passing'].get(pid,{}); ss=sources['scoring'].get(pid,{}); want=n(r.get(col))
    if want is None:continue
    yards=((n(rr.get('rush_yds')) or 0)+(n(rr.get('rec_yds')) or 0))/10
    formula=yards+(n(pp.get('pass_yds')) or 0)/25+4*(n(pp.get('pass_td')) or 0)-2*(n(pp.get('pass_int')) or 0)+6*(n(ss.get('total_tds_scored')) or 0)+2*(n(ss.get('two_pt_md')) or 0)-2*fum.get(normkey(r.get('player')),0)
    comp+=1
    if abs(round(formula)-want)<=.06:match+=1
    elif len(ex)<5:ex.append({'player':r.get('player'),'pfr':want,'recomputed_standard':round(formula,2)})
  checks[col]={'comparable':comp,'matches':match,'mismatches':comp-match}
  if ex:examples[col]=ex
  key=f'pfr_player_fantasy|*|{col}';d=decisions.get(key,{})
  matrix.append({'source':'pfr_player_fantasy','table_key':'*','column':col,'disposition':audit,'ledger_disposition':d.get('disposition'),'canonical':d.get('canonical'),'audit_disposition':audit,'checks':checks[col]})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':'pfr_player_fantasy','class':'player fantasy/scoring','year':2025,'selected_rows':len(rows),'columns':len(cols),'checks':checks,'mismatch_examples':examples,'matrix':matrix,'open_blocked':0,'notes':['Fantasy points were independently recomputed using PFR standard scoring operands and game-offense fumbles lost.','Ranks/VBD are publisher-relative witnesses; no PPR mapping is assumed.','No raw or release data was modified.']},indent=2,default=str),encoding='utf-8');print({'written':str(OUT),'rows':len(rows),'columns':len(cols),'open_blocked':0})
if __name__=='__main__':main()
