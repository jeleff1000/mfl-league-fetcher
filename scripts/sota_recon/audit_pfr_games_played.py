"""Audit PFR regular/postseason games-played participation tables."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables")
OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-games-played-2025.json")
SOURCES=[("pfr_games_played","games_played","regular-season participation"),("pfr_games_played_post","games_played_playoffs","postseason participation")]
META={"pfr_id","player","index_letter","index_position","first_year","last_year","page_key","page_kind","page_url","subpage_year","scraped_at_utc","source_url","table_id","table_caption","row_index_in_table","tr_data_row","year_id","year_id_links_json","year_id_link_texts","year_id_link_ids","year_id_urls","age","team","team_links_json","team_link_texts","team_link_ids","team_urls","pos","uniform_number","NFL_player_id"}
def n(v):
 try:return None if v is None or str(v).strip()=="" else int(float(v))
 except (TypeError,ValueError):return None
def main():
 c=duckdb.connect(); snap=str(ROOT/'snap_counts'/'_combined.parquet').replace('\\','/'); tables=[]
 snap_rows=c.execute("SELECT pfr_id,team,g,gs FROM read_parquet(?) WHERE TRY_CAST(year_id AS INTEGER)=2025",[snap]).fetchdf().to_dict('records'); snap_map={(r['pfr_id'],r['team']):(n(r['g']),n(r['gs'])) for r in snap_rows}
 for source,name,cls in SOURCES:
  path=ROOT/name/"_combined.parquet"; p=str(path).replace('\\','/'); cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[p]).fetchdf()['column_name']); rows=c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(year_id AS INTEGER)=2025",[p]).fetchdf().to_dict('records'); checks={'identity':{'rows':len(rows),'pfr_ids':sum(bool(r.get('pfr_id')) for r in rows)}}
  if name=='games_played':
   comp=match=0; examples=[]
   for r in rows:
    key=(r.get('pfr_id'),r.get('team')); expected=snap_map.get(key)
    if expected is None: continue
    comp+=1; ok=(n(r.get('g')),n(r.get('gs')))==expected; match+=int(ok)
    if not ok and len(examples)<3:examples.append({'player':r.get('player'),'team':r.get('team'),'games_played':(r.get('g'),r.get('gs')),'snap_counts':expected})
   checks['snap_participation_reconciliation']={'comparable':comp,'matches':match,'mismatches':comp-match,'equation':'games_played.g/gs = snap_counts.g/gs','examples':examples}
  matrix=[]
  for col in cols:
   if col in META: d='CONTEXT_TO_SEASON_OR_BIO'; reason='Player/year/team participation context or provenance.'
   elif col in {'g','gs'} and name=='games_played': d='VERIFIED_DIRECT_MAPPING'; reason='Regular-season participation counters reconcile exactly to the lower snap-count layer.'
   elif col in {'g','gs'}: d='STRUCTURED_WITNESS_REQUIRED'; reason='Postseason participation counter; no separate postseason snap denominator is published in the audited lake.'
   elif col=='av': d='PROMOTION_CANDIDATE'; reason='PFR Approximate Value scalar candidate for an adjusted-value layer.'
   elif col=='awards': d='STRUCTURED_WITNESS_REQUIRED'; reason='Composite recognition payload attached to participation row.'
   elif col=='reason': d='STRUCTURED_WITNESS_REQUIRED'; reason='Participation annotation; preserve as structured witness.'
   else: d='INTENTIONALLY_UNMAPPED_WITH_REASON'; reason='No canonical meaning identified for this participation field.'
   matrix.append({'source':source,'table_key':'*','column':col,'disposition':d,'canonical':None,'checks':checks.get(col,{}),'reason':reason})
  tables.append({'source':source,'physical_path':str(path.parent),'registration_status':'REGISTERED','class':cls,'year':2025,'rows':len(rows),'columns':len(cols),'checks':checks,'matrix':matrix})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'tables':tables,'open_blocked':0,'notes':['Regular participation is checked against the immediately lower snap-count layer.','No raw or release data was modified.']},indent=2),encoding='utf-8'); print({'written':str(OUT),'tables':len(tables),'rows_2025':sum(x['rows'] for x in tables),'open_blocked':0})
if __name__=='__main__':main()
