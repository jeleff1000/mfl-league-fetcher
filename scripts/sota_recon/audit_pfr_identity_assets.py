"""Receipts for PFR identity/index assets and physical probe/smoke bundles."""
from __future__ import annotations
import json
from datetime import datetime,timezone
from pathlib import Path
import duckdb
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players");OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-identity-assets-2025.json")
def matrix(source,cols,reason):
 return [{'source':source,'table_key':'*','column':c,'disposition':'CONTEXT_OR_PROVENANCE','canonical':None,'reason':reason,'evidence':'PFR identity asset on local immutable lake'} for c in cols]
def main():
 c=duckdb.connect();tables=[]
 for name in ['player_index.parquet','player_subpage_index.parquet']:
  p=str(ROOT/name).replace('\\','/');cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[p]).fetchdf()['column_name']);rows=c.execute('SELECT * FROM read_parquet(?)',[p]).fetchdf().to_dict('records');
  if name=='player_index.parquet':
   unique=len({r.get('pfr_id') for r in rows});null_ids=sum(not r.get('pfr_id') for r in rows);checks={'rows':len(rows),'unique_pfr_ids':unique,'null_pfr_ids':null_ids,'duplicate_ids':len(rows)-unique}
   reason='Stable PFR player identity/index field; identity witness, not a statistical scalar.'
  else:
   unique=len({(r.get('source_pfr_id'),r.get('page_key'),r.get('subpage_year'),r.get('url')) for r in rows});checks={'rows':len(rows),'unique_subpage_links':unique,'duplicate_links':len(rows)-unique};reason='Subpage identity/crosswalk witness; links season/player page surfaces and does not map to a stat scalar.'
  tables.append({'source':f'pfr_player_{name[:-8]}','physical_path':str(ROOT/name),'registration_status':'REGISTERED','rows':len(rows),'columns':len(cols),'checks':checks,'matrix':matrix(f'pfr_player_{name[:-8]}',cols,reason)})
 for bundle in ['pfr_players_probe','pfr_players_smoke']:
  files=[str(x) for x in (ROOT/bundle).rglob('*') if x.is_file()];tables.append({'source':bundle,'physical_path':str(ROOT/bundle),'registration_status':'PHYSICAL_UNREGISTERED_RECOVERY_BUNDLE','rows':None,'columns':None,'checks':{'files':len(files),'2025_comparison':0},'matrix':[{'source':bundle,'table_key':'physical','column':'*file_bundle*','disposition':'INTENTIONALLY_UNMAPPED_WITH_REASON','canonical':None,'reason':'Recovery/probe bundle, not a registered production PFR table; preserve for provenance and do not promote fields without a production schema contract.','evidence':str(ROOT/bundle)}]})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'class':'player identity/index','tables':tables,'open_blocked':0,'notes':['Identity assets are crosswalk witnesses; stable pfr_id is the primary key.','No raw or release data was modified.']},indent=2),encoding='utf-8');print({'written':str(OUT),'tables':len(tables),'open_blocked':0})
if __name__=='__main__':main()
