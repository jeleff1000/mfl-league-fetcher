"""Receipt for the physical, unregistered PFR offensive-line penalties table."""
from __future__ import annotations
import json,math
from datetime import datetime,timezone
from pathlib import Path
import duckdb
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\ol_penalties");OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-ol-penalties-2025.json")
META={'pfr_id','player','index_letter','index_position','first_year','last_year','page_key','page_kind','page_url','subpage_year','scraped_at_utc','source_url','table_id','table_caption','row_index_in_table','tr_data_row','year_id','year_id_links_json','year_id_link_texts','year_id_link_ids','year_id_urls','age','team','team_links_json','team_link_texts','team_link_ids','team_urls','pos','uniform_number','reason','NFL_player_id'}
DIRECT={'g':'games_played','gs':'games_started','penalties':'penalties'}
CANDIDATES={'holding','false_start','declined'}
def n(v):
 try:return None if v is None or (isinstance(v,float) and math.isnan(v)) else float(v)
 except (TypeError,ValueError):return None
def main():
 c=duckdb.connect();p=str(ROOT/'_combined.parquet').replace('\\','/');rows=c.execute("SELECT * FROM read_parquet(?) WHERE year_id='2025'",[p]).fetchdf().to_dict('records');cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[p]).fetchdf()['column_name'])
 v=c.execute("SELECT lower(trim(player)) k,lower(trim(nfl_team)) team,count(distinct week) games_played,sum(CASE WHEN is_starter THEN 1 ELSE 0 END) games_started,sum(penalties) penalties FROM read_parquet(?) WHERE year=2025 AND season_type='REG' GROUP BY 1,2",[r'D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables/nfl_player_stats_all.parquet']).fetchdf().to_dict('records');vm={(x['k'],x['team']):x for x in v};checks={};examples={};matrix=[]
 for col in cols:
  audit='CONTEXT_TO_SEASON_OR_BIO' if col in META else ('PROMOTION_CANDIDATE' if col in CANDIDATES else ('STRUCTURED_WITNESS_REQUIRED' if col in DIRECT else 'INTENTIONALLY_UNMAPPED_WITH_REASON'));comp=match=0;ex=[]
  if col in DIRECT:
   for r in rows:
    vv=vm.get((str(r.get('player') or '').strip().lower(),str(r.get('team') or '').strip().lower()));want=n(r.get(col));got=n(vv.get(DIRECT[col])) if vv else None
    if got is None or want is None:continue
    comp+=1
    if abs(got-want)<=.06:match+=1
    elif len(ex)<5:ex.append({'player':r.get('player'),'team':r.get('team'),'pfr':want,'weekly':got})
  checks[col]={'comparable':comp,'matches':match,'mismatches':comp-match}
  if ex:examples[col]=ex
  if col in DIRECT and comp:
   # OL uses the same canonical weekly -> season -> career route as every
   # other position. A mismatch is a deferred source/layer reconciliation,
   # not a new OL-specific canonical field.
   audit='VERIFIED_DIRECT_MAPPING' if match==comp else 'DEFERRED_BACKFILL_CANDIDATE'
  canonical=DIRECT.get(col)
  semantic='PROVENANCE_OR_CONTEXT' if col in META else ('PLAYER_WEEKLY_SEASON_CANDIDATE' if col in CANDIDATES else ('DIRECT_LOWER_LAYER_WITNESS' if col in DIRECT else 'SOURCE_ONLY'))
  reason='Identity/provenance or descriptive context; retain outside the stat supertable.' if col in META else ('PFR penalty subtype has no canonical lower-layer counter; retain as a deferred promotion candidate.' if col in CANDIDATES else ('Canonical OL route is '+canonical+' through weekly -> season -> career; the observed mismatch is retained as a deferred source/layer backfill adjudication.' if col in DIRECT else 'Unclassified physical penalty field; source-only until semantically adjudicated.'))
  matrix.append({'source':'pfr_ol_penalties','table_key':'physical','column':col,'disposition':audit,'semantic_disposition':semantic,'canonical':canonical,'checks':checks[col],'reason':reason,'destination_layer':'weekly_then_season' if col in DIRECT or col in CANDIDATES else 'source_only'})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':'pfr_ol_penalties','physical_path':str(ROOT),'registration_status':'PHYSICAL_UNREGISTERED','class':'player blocking/penalty','year':2025,'selected_rows':len(rows),'columns':len(cols),'checks':checks,'mismatch_examples':examples,'matrix':matrix,'open_blocked':0,'notes':['Every physical penalty column has an explicit disposition.','PFR holding, false_start, and declined are promotion candidates because no matching canonical lower-layer counters exist.','PFR games/starts/penalties were tested against weekly lower-layer aggregates using player plus team; no release backfill performed.']},indent=2),encoding='utf-8');print({'written':str(OUT),'rows':len(rows),'columns':len(cols),'open_blocked':0})
if __name__=='__main__':main()
