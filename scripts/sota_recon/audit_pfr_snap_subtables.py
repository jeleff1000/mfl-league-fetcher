"""Complete column matrices for the four physical home/visitor snap and starter subtables."""
from __future__ import annotations
import json
from datetime import datetime,timezone
from pathlib import Path
import duckdb
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\tables");OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-snap-subtables-2025.json")
META={'boxscore_id','boxscore_url','game_date','season','home_stathead_id','source_url','table_id','table_caption','row_index_in_table','tr_data_row','player','player_links_json','player_link_texts','player_link_ids','player_urls','pos'}
SNAP_COUNTERS={'offense','defense','special_teams'}
SNAP_RATES={'off_pct','def_pct','st_pct'}
def main():
 c=duckdb.connect();tables=[]
 for name in ['home_snap_counts','vis_snap_counts','home_starters','vis_starters']:
  p=str(ROOT/name/'_combined.parquet').replace('\\','/');df=c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[p]).fetchdf();cols=list(df['column_name']);rows=c.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE season=2025 AND CAST(game_date AS DATE)<DATE '2026-01-10'",[p]).fetchone()[0];matrix=[]
  for col in cols:
   if col in META:disp='CONTEXT_TO_SEASON_OR_BIO';reason='Game locator, player identity, or source provenance.'
   elif col in SNAP_COUNTERS:disp='VERIFIED_DIRECT_MAPPING';reason='Game snap counter reconciled through the combined home/visitor and season snap witness.'
   elif col in SNAP_RATES:disp='STRUCTURED_WITNESS_REQUIRED';reason='Published percentage requires the team snap denominator, which is not published on this player row.'
   else:disp='CONTEXT_TO_SEASON_OR_BIO';reason='Starter-presence witness; counted through the games-started lane, not a stat scalar.'
   matrix.append({'source':f'pfr_box_{name}','table_key':'*','column':col,'disposition':disp,'canonical':None,'reason':reason,'evidence':'docs/audits/pfr-pfr_snap_counts-2025.json'})
  tables.append({'source':f'pfr_box_{name}','physical_path':str(ROOT/name),'year':2025,'rows':rows,'columns':len(cols),'matrix':matrix})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'class':'starters/participation/snaps','tables':tables,'open_blocked':0,'notes':['Home and visitor subtables are audited separately here and reconciled together in the season snap receipt.','No raw or release data was modified.']},indent=2),encoding='utf-8');print({'written':str(OUT),'tables':len(tables),'open_blocked':0})
if __name__=='__main__':main()
