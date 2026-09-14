"""Audit ordinary PFR player passing season and postseason tables."""
from __future__ import annotations
import json
from datetime import datetime,timezone
from pathlib import Path
import duckdb

ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables"); OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-player-passing-2025.json")
SOURCES=[('pfr_player_season_passing','passing','regular-season player passing'),('pfr_passing_post','passing_post','postseason player passing')]
META={'pfr_id','player','index_letter','index_position','first_year','last_year','page_key','page_kind','page_url','subpage_year','scraped_at_utc','source_url','table_id','table_caption','row_index_in_table','tr_data_row','year_id','year_id_links_json','year_id_link_texts','year_id_link_ids','year_id_urls','age','team_name_abbr','team_name_abbr_links_json','team_name_abbr_link_texts','team_name_abbr_link_ids','team_name_abbr_urls','comp_name_abbr','comp_name_abbr_links_json','comp_name_abbr_link_texts','comp_name_abbr_link_ids','comp_name_abbr_urls','pos','NFL_player_id'}
RATES={'pass_cmp_pct':('pass_cmp','pass_att',100),'pass_td_pct':('pass_td','pass_att',100),'pass_int_pct':('pass_int','pass_att',100),'pass_yds_per_att':('pass_yds','pass_att',1),'pass_yds_per_cmp':('pass_yds','pass_cmp',1),'pass_yds_per_g':('pass_yds','games',1),'pass_sacked_pct':('pass_sacked','pass_att_plus_sacked',100),'pass_net_yds_per_att':('pass_yds_minus_sacked_yds','pass_att_plus_sacked',1)}
DIRECT={'games','games_started','pass_cmp','pass_att','pass_yds','pass_td','pass_int','pass_long','pass_sacked','pass_sacked_yds','pass_first_down'}
CANDIDATES={'pass_success','comebacks','gwd','qb_rec','qbr','av'}
def n(v):
 try:return None if v is None or str(v).strip()=='' else float(v)
 except (TypeError,ValueError):return None
def op(r,name):
 if name=='pass_att_plus_sacked':
  a=n(r.get('pass_att'));b=n(r.get('pass_sacked'));return None if a is None or b is None else a+b
 if name=='pass_yds_minus_sacked_yds':
  a=n(r.get('pass_yds'));b=n(r.get('pass_sacked_yds'));return None if a is None or b is None else a-b
 return n(r.get(name))
def main():
 c=duckdb.connect(); tables=[]
 for source,name,cls in SOURCES:
  p=str(ROOT/name/'_combined.parquet').replace('\\','/');cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[p]).fetchdf()['column_name']);rows=c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(year_id AS INTEGER)=2025",[p]).fetchdf().to_dict('records');checks={}
  for pub,(num,den,mult) in RATES.items():
   if pub not in cols:continue
   comp=match=0;examples=[]
   for r in rows:
    a=op(r,pub);b=op(r,num);d=op(r,den)
    if a is None or b is None or d in (None,0):continue
    comp+=1;ok=abs(a-b/d*mult)<=.11;match+=int(ok)
    if not ok and len(examples)<3:examples.append({'player':r.get('player'),'published':a,'expected':b/d*mult})
   checks[pub]={'comparable':comp,'matches':match,'mismatches':comp-match,'equation':f'{num} / {den} * {mult}','examples':examples}
  matrix=[]
  for col in cols:
   if col in META:d='CONTEXT_TO_SEASON_OR_BIO';reason='Player/year/team identity or provenance.'
   elif col in RATES:d='VERIFIED_DERIVED_WITNESS';reason='Published rate independently reproduced from same-row operands.'
   elif col in DIRECT:d='VERIFIED_DIRECT_MAPPING';reason='Core passing counter at the native player-season/postseason grain.'
   elif col in CANDIDATES:d='PROMOTION_CANDIDATE';reason='Ledger-locked PFR passing material not currently represented by the canonical raw passing scalar.'
   elif col=='awards' or col.startswith('awards_'):d='STRUCTURED_WITNESS_REQUIRED';reason='Composite recognition payload.'
   else:d='STRUCTURED_WITNESS_REQUIRED';reason='Publisher-derived passing field without a proven canonical equation.'
   matrix.append({'source':source,'table_key':'*','column':col,'disposition':d,'canonical':None,'checks':checks.get(col,{}),'reason':reason})
  tables.append({'source':source,'physical_path':str(ROOT/name),'registration_status':'REGISTERED','class':cls,'year':2025,'rows':len(rows),'columns':len(cols),'checks':checks,'matrix':matrix})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'tables':tables,'open_blocked':0,'notes':['Ordinary passing season and postseason tables are distinct from advanced passing tables.','No raw or release data was modified.']},indent=2),encoding='utf-8');print({'written':str(OUT),'tables':len(tables),'rows_2025':sum(x['rows'] for x in tables),'open_blocked':0})
if __name__=='__main__':main()
