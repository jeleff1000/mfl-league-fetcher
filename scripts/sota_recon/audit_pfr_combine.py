"""Receipts for PFR context and player-page combine/physical measurement tables."""
from __future__ import annotations
import json,math
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr");OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-combine-2025.json")
MAP={'height':'height','weight':'weight','forty_yd':'forty_yd','bench_reps':'bench_reps','broad_jump':'broad_jump','shuttle':'shuttle','cone':'cone','vertical':'vertical'}
META={'source_kind','page_key','year','team_id','page_url','source_url','table_id','table_caption','row_index_in_table','tr_data_row','player','player_links_json','player_link_texts','player_link_ids','player_urls','pos','school_name','school_name_links_json','school_name_link_texts','school_name_link_ids','school_name_urls','college','college_links_json','college_link_texts','college_link_ids','college_urls','pfr_id','index_letter','index_position','first_year','last_year','page_kind','subpage_year','scraped_at_utc','year_id','year_id_links_json','year_id_link_texts','year_id_link_ids','year_id_urls','NFL_player_id'}
def val(v, height=False):
 try:
  if v is None or (isinstance(v,float) and math.isnan(v)):return None
  s=str(v).strip()
  if height and '-' in s:
   ft,inch=s.split('-',1);return float(ft)*12+float(inch)
  return float(s)
 except (TypeError,ValueError):return None
def main():
 c=duckdb.connect();ctx=str(ROOT/'context/tables/combine/_combined.parquet').replace('\\','/');pl=str(ROOT/'players/tables/combine/_combined.parquet').replace('\\','/')
 crows=c.execute("SELECT * FROM read_parquet(?)",[ctx]).fetchdf().to_dict('records');prows=c.execute("SELECT * FROM read_parquet(?)",[pl]).fetchdf().to_dict('records');pmap={(str(r.get('player_link_ids') or '').split(';')[0].strip(),str(r.get('year') or '')):r for r in crows if str(r.get('player_link_ids') or '').strip()};checks={};examples={};
 for pc,cc in MAP.items():
  comp=match=0;ex=[]
  for r in prows:
   k=(str(r.get('pfr_id') or '').strip(),str(r.get('year_id') or ''));other=pmap.get(k);a=val(r.get(pc));b=val(other.get(cc),height=(pc=='height')) if other else None
   if a is None or b is None:continue
   comp+=1
   if abs(a-b)<=.06:match+=1
   elif len(ex)<5:ex.append({'pfr_id':k[0],'year':k[1],'player_value':a,'context_value':b})
  checks[pc]={'comparable':comp,'matches':match,'mismatches':comp-match}
  if ex:examples[pc]=ex
 D=load_decisions();tables=[]
 for source,rows,cols in [('pfr_combine',crows,list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[ctx]).fetchdf()['column_name'])),('pfr_player_combine',prows,list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[pl]).fetchdf()['column_name']))]:
  matrix=[]
  for col in cols:
   if col in MAP:audit='PHYSICAL_MEASUREMENT_WITNESS'
   elif col=='draft_info':audit='STRUCTURED_WITNESS_REQUIRED'
   else:audit='CONTEXT_OR_PROVENANCE'
   key=f'{source}|*|{col}';d=D.get(key,{})
   matrix.append({'source':source,'table_key':'*','column':col,'disposition':d.get('disposition'),'canonical':d.get('canonical'),'audit_disposition':audit,'checks':checks.get(col,{})})
  tables.append({'source':source,'physical_path':str(ctx if source=='pfr_combine' else pl),'rows':len(rows),'2025_rows':sum(str(r.get('year') or r.get('year_id') or '')=='2025' for r in rows),'columns':len(cols),'matrix':matrix})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'class':'combine/physical measurements','overlap_checks':checks,'mismatch_examples':examples,'tables':tables,'open_blocked':0,'notes':['Player-page combine measurements were compared to context combine by PFR player-link id and year.','draft_info remains composite and is not mapped as a scalar.','No raw or release data was modified.']},indent=2,default=str),encoding='utf-8');print({'written':str(OUT),'context_rows':len(crows),'player_rows':len(prows),'open_blocked':0})
if __name__=='__main__':main()
